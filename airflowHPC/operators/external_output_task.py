from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.external_task import ExternalTaskSensor, ExternalDagLink
from airflow.triggers.external_task import TaskStateTrigger
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.timezone import utcnow

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.utils.context import Context


class ExternalTaskOutputSensor(ExternalTaskSensor):
    """
    Like the ExternalTaskSensor, but it returns the outputs of the external tasks.
    """

    template_fields = [
        "external_dag_id",
        "external_task_id",
        "external_task_ids",
        "external_task_group_id",
    ]
    ui_color = "#ed900e"
    operator_extra_links = [ExternalDagLink()]

    def execute(self, context: Context) -> None:
        """Run on the worker and defer using the triggers if deferrable is set to True."""
        if not self.deferrable:
            return super(ExternalTaskSensor, self).execute(context)
        else:
            self.defer(
                trigger=TaskStateTrigger(
                    dag_id=self.external_dag_id,
                    task_id=self.external_task_id,
                    execution_dates=self._get_dttm_filter(context),
                    states=self.allowed_states,
                    trigger_start_time=utcnow(),
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

    @provide_session
    def poke(
        self, context: Context, session: Session = NEW_SESSION
    ) -> PokeReturnValue | bool:
        # delay check to poke rather than __init__ in case it was supplied as XComArgs
        if self.external_task_ids and len(self.external_task_ids) > len(
            set(self.external_task_ids)
        ):
            raise ValueError("Duplicate task_ids passed in external_task_ids parameter")

        dttm_filter = self._get_dttm_filter(context)
        serialized_dttm_filter = ",".join(dt.isoformat() for dt in dttm_filter)

        if self.external_task_ids:
            self.log.info(
                "Poking for tasks %s in dag %s on %s ... ",
                self.external_task_ids,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if self.external_task_group_id:
            self.log.info(
                "Poking for task_group '%s' in dag '%s' on %s ... ",
                self.external_task_group_id,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if (
            self.external_dag_id
            and not self.external_task_group_id
            and not self.external_task_ids
        ):
            self.log.info(
                "Poking for DAG '%s' on %s ... ",
                self.external_dag_id,
                serialized_dttm_filter,
            )

        # In poke mode this will check dag existence only once
        if self.check_existence and not self._has_checked_existence:
            self._check_for_existence(session=session)

        count_failed = -1
        if self.failed_states:
            count_failed = self.get_count(dttm_filter, session, self.failed_states)

        # Fail if anything in the list has failed.
        if count_failed > 0:
            if self.external_task_ids:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"Some of the external tasks {self.external_task_ids} "
                        f"in DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} failed."
                )
            elif self.external_task_group_id:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external task_group '{self.external_task_group_id}' "
                        f"in DAG '{self.external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG '{self.external_dag_id}' failed."
                )

            else:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"The external DAG {self.external_dag_id} failed."
                )

        count_skipped = -1
        if self.skipped_states:
            count_skipped = self.get_count(dttm_filter, session, self.skipped_states)

        # Skip if anything in the list has skipped. Note if we are checking multiple tasks and one skips
        # before another errors, we'll skip first.
        if count_skipped > 0:
            if self.external_task_ids:
                raise AirflowSkipException(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            elif self.external_task_group_id:
                raise AirflowSkipException(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            else:
                raise AirflowSkipException(
                    f"The external DAG {self.external_dag_id} reached a state in our states-to-skip-on list. "
                    "Skipping."
                )

        # only go green if every single task has reached an allowed state
        count_allowed = self.get_count(dttm_filter, session, self.allowed_states)
        if count_allowed != len(dttm_filter):
            return False
        else:
            task_outputs = self.get_task_outputs(session, self.allowed_states)
            self.log.info(f"Task outputs: {task_outputs}")
            return PokeReturnValue(is_done=True, xcom_value=task_outputs)

    def get_task_outputs(self, session, states) -> Any:
        TI = TaskInstance

        task_outputs = []
        if self.external_task_ids:
            dag_runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == self.external_dag_id)
                .all()
            )
            for dag_run in dag_runs:
                task_instances = session.query(TI).filter(
                    TI.dag_id == self.external_dag_id,
                    TI.task_id.in_(self.external_task_ids),
                    TI.execution_date.in_([dag_run.execution_date]),
                    TI.state.in_(states),
                )
                for task_instance in task_instances:
                    outputs = task_instance.xcom_pull(task_ids=task_instance.task_id)
                    for output in outputs:
                        task_outputs.append(output)
        return task_outputs
