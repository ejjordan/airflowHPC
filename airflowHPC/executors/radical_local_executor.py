from __future__ import annotations

import queue
import contextlib
import sys
from typing import TYPE_CHECKING, Any, Optional, Tuple, Sequence

from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

import radical.pilot as rp
import logging


if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import (
        SimpleTaskInstance,
        TaskInstance,
    )
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die
    # instantly.
    ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]

    # Task tuple to send to be executed
    TaskTuple = Tuple[TaskInstanceKey, CommandType, Optional[str], Optional[Any]]
    QueuedTaskInstanceType = Tuple[CommandType, int, Optional[str], TaskInstance]
    EventBufferValueType = Tuple[Optional[str], Any]


class RadicalExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = False

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)

        self.log.info(f"=== RadicalExecutor: __init__ {parallelism}")
        self._rp_keys = None
        self._rp_results = None
        self._rp_session = None
        self._rp_log = None
        self._rp_pmgr = None
        self._rp_tmgr = None

    def start(self) -> None:
        """Start the executor."""
        self._rp_keys = dict()
        self._rp_results = queue.Queue()
        self._rp_session = rp.Session()
        self._rp_log = logging  # self._rp_session._log
        self._rp_pmgr = rp.PilotManager(session=self._rp_session)
        self._rp_tmgr = rp.TaskManager(session=self._rp_session)
        self._rp_env_name = "rp"

        self._rp_log.info(f"=== RadicalExecutor: start")
        pd = rp.PilotDescription(
            {"resource": "local.localhost", "cores": self.parallelism, "runtime": 30}
        )
        pilot = self._rp_pmgr.submit_pilots(pd)
        if sys.prefix != sys.base_prefix:
            # TODO: make this an airflow configuration variable
            self._rp_env_name = "local_venv"
            env_spec = {"type": "venv", "path": sys.prefix, "setup": []}
            pilot.prepare_env(env_name=self._rp_env_name, env_spec=env_spec)

        self._rp_tmgr.add_pilots(pilot)

        def state_cb(task, state):
            tid = task.uid
            self._rp_log.info(f"=== {tid}: {state}")
            if state in rp.FINAL:
                key = self._rp_keys.pop(tid)
                if state == rp.DONE:
                    self._rp_results.put((key, TaskInstanceState.FAILED))
                else:
                    self._rp_results.put((key, TaskInstanceState.SUCCESS))

        self._rp_tmgr.register_callback(state_cb)

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        from airflow.utils.cli import get_dag
        import os

        self._rp_log.info(f"=== execute_async {key}: {command}")

        dag = get_dag(dag_id=key.dag_id, subdir=os.path.join("dags", key.dag_id))
        task = dag.get_task(key.task_id)
        # Raise if the task does not have output_files - TODO: handle this in the task decorator
        if "output_files" not in task.op_kwargs:
            raise AttributeError(f"Task {task} does not have output_files")
        rp_out_paths = [
            os.path.join(task.op_kwargs["output_dir"], v)
            for k, v in task.op_kwargs["output_files"].items()
        ]

        self.validate_airflow_tasks_run_command(command)
        td = rp.TaskDescription()
        td.executable = command[0]
        td.arguments = command[1:]
        td.metadata = {"key": key}
        td.named_env = self._rp_env_name
        td.output_staging = [
            {
                "source": f"task:///{out_path}",
                "target": f"client:///{out_path}",
                "action": rp.COPY,
            }
            for out_path in rp_out_paths
        ]
        logging.info(f"=== output_staging: {td.output_staging}")

        task = self._rp_tmgr.submit_tasks(td)

        self._rp_keys[task.uid] = key
        self._rp_log.info(f"=== submitted task: {task}")

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(queue.Empty):
            while True:
                results = self._rp_results.get_nowait()
                try:
                    self.change_state(*results)
                finally:
                    self._rp_results.task_done()

    def end(self) -> None:
        self._rp_log.info(f"=== RadicalExecutor: end")
        self._rp_session.close()


class RadicalLocalExecutor(LoggingMixin):
    is_local: bool = True
    is_single_threaded: bool = False
    is_production: bool = True

    serve_logs: bool = True

    RADICAL_QUEUE = "radical"

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__()
        self._job_id: str | None = None
        self.local_executor = LocalExecutor(parallelism=parallelism)
        self.radical_executor = RadicalExecutor(parallelism=parallelism)

    @property
    def queued_tasks(self) -> dict[TaskInstanceKey, QueuedTaskInstanceType]:
        """Return queued tasks from local and radical executor."""
        queued_tasks = self.local_executor.queued_tasks.copy()
        queued_tasks.update(self.radical_executor.queued_tasks)
        logging.info(f"Queued tasks: {queued_tasks}")

        return queued_tasks

    @property
    def running(self) -> set[TaskInstanceKey]:
        """Return running tasks from local and radical executor."""
        return self.local_executor.running.union(self.radical_executor.running)

    @property
    def slots_available(self) -> int:
        """Number of new tasks this executor instance can accept."""
        return self.local_executor.slots_available

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: str | None = None,
    ) -> None:
        executor = self._router(task_instance)
        logging.info(
            "Using executor: %s for %s", executor.__class__.__name__, task_instance.key
        )
        executor.queue_command(task_instance, command, priority, queue)
        logging.info(
            f"Executor: {executor} Queued command: {command} for task: {task_instance.key}"
        )

    def _router(
        self, simple_task_instance: SimpleTaskInstance
    ) -> LocalExecutor | RadicalExecutor:
        logging.info(f"Routing to queue: {simple_task_instance.queue}")
        if simple_task_instance.queue == self.RADICAL_QUEUE:
            return self.radical_executor
        return self.local_executor

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        pickle_id: int | None = None,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> None:
        """Queues task instance via local or radical executor."""
        from airflow.models.taskinstance import SimpleTaskInstance

        executor = self._router(SimpleTaskInstance.from_ti(task_instance))
        self.log.debug(
            "Using executor: %s to queue_task_instance for %s",
            executor.__class__.__name__,
            task_instance.key,
        )
        executor.queue_task_instance(
            task_instance=task_instance,
            mark_success=mark_success,
            pickle_id=pickle_id,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            cfg_path=cfg_path,
        )

    def start(self) -> None:
        logging.info("Starting local and Radical Executor")
        self.local_executor.start()
        self.radical_executor.start()

    def end(self) -> None:
        logging.info("Ending local and Radical Executor")
        self.local_executor.end()
        self.radical_executor.end()

    def has_task(self, task_instance: TaskInstance) -> bool:
        """Checks if a task is either queued or running in either local or radical executor."""
        return self.local_executor.has_task(
            task_instance
        ) or self.radical_executor.has_task(task_instance)

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs in local and radical executor."""
        self.local_executor.heartbeat()
        self.radical_executor.heartbeat()

    def get_event_buffer(
        self, dag_ids: list[str] | None = None
    ) -> dict[TaskInstanceKey, EventBufferValueType]:
        """Return and flush the event buffer from local and radical executor."""
        cleared_events_from_local = self.local_executor.get_event_buffer(dag_ids)
        cleared_events_from_radical = self.radical_executor.get_event_buffer(dag_ids)

        return {**cleared_events_from_local, **cleared_events_from_radical}

    def try_adopt_task_instances(
        self, tis: Sequence[TaskInstance]
    ) -> Sequence[TaskInstance]:
        """
        Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

        Anything that is not adopted will be cleared by the scheduler (and then become eligible for
        re-scheduling)

        :return: any TaskInstances that were unable to be adopted
        """
        local_tis = [ti for ti in tis if ti.queue != self.RADICAL_QUEUE]
        radical_tis = [ti for ti in tis if ti.queue == self.RADICAL_QUEUE]
        return [
            *self.local_executor.try_adopt_task_instances(local_tis),
            *self.radical_executor.try_adopt_task_instances(radical_tis),
        ]

    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        raise NotImplementedError()

    @staticmethod
    def get_cli_commands() -> list:
        return BaseExecutor.get_cli_commands()
