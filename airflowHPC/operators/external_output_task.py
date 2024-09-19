from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
        poke_value = super().poke(context)
        if poke_value is False:
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
