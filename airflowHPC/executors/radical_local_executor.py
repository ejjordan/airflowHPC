from __future__ import annotations

import queue
import contextlib
import sys
import time
from typing import TYPE_CHECKING, Any, Optional, Tuple, Sequence

from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

import radical.utils as ru
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
        tic = time.perf_counter()
        self._rp_keys = dict()
        self._rp_results = queue.Queue()
        self._rp_session = rp.Session()
        self._rp_log = logging  # TODO: should this be self._rp_session._log instead?
        self._rp_pmgr = rp.PilotManager(session=self._rp_session)
        self._rp_tmgr = rp.TaskManager(session=self._rp_session)
        self._rp_env_name = "rp"

        self._rp_log.info("=== RadicalExecutor: start")
        # TODO: make resource and runtime airflow configuration variables or expose in some way
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
        toc = time.perf_counter()
        self._rp_log.info(f"=== RadicalExecutor: start took {toc - tic:0.4f} seconds")

        def state_cb(task, state):
            tid = task.uid
            self._rp_log.info(f"=== {tid}: {state}")
            if state in rp.FINAL:
                key = self._rp_keys.pop(tid)
                self._rp_log.info(
                    f"=== {tid}: DAG {key.dag_id}; Task {key.task_id}; {state}"
                )
                if state == rp.DONE:
                    self._rp_results.put((key, TaskInstanceState.FAILED))
                else:
                    self._rp_results.put((key, TaskInstanceState.SUCCESS))

        self._rp_tmgr.register_callback(state_cb)

        # start a service endpoint which listens for task execution requests
        # sent from the main airflow command line process
        self._zmq_server = ru.zmq.Server()
        self._zmq_server.register_request("rp_execute", self._rp_execute)
        self._zmq_server.start()

        self._rp_endpoint = self._zmq_server.addr()


    def _rp_execute(self, task_description: dict,
                    key: TaskInstanceKey) -> None:

        td   = rp.TaskDescription(task_description)
        task = self._rp_tmgr.submit_tasks(td)

        self._rp_keys[task.uid] = key

        self._rp_log.info(f"=== submitted task: {task}")

        return task.uid


    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:

        self._rp_log.info(f"=== execute_async {key}: {command}")

        self.validate_airflow_tasks_run_command(command)

        bash_cmd = 'RP_ENDPOINT=%s airflow tasks run %s %s' \
                 % self._rp_endpoint, key.dag_id, key.task_id
        ru.sh_callout_bg("bash -c '%s'" % bash_cmd)



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
        self._rp_log.info("=== RadicalExecutor: end")
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
