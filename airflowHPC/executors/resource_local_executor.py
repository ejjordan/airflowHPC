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

import logging

from airflowHPC.executors.resource_executor import ResourceExecutor

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


class ResourceLocalExecutor(LoggingMixin):
    is_local: bool = True
    is_single_threaded: bool = False
    is_production: bool = True

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__()
        self._job_id: str | None = None
        self.local_executor = LocalExecutor(parallelism=parallelism)
        self.resource_executor = ResourceExecutor()

    @property
    def queued_tasks(self) -> dict[TaskInstanceKey, QueuedTaskInstanceType]:
        """Return queued tasks from local and resource executor."""
        queued_tasks = self.local_executor.queued_tasks.copy()
        queued_tasks.update(self.resource_executor.queued_tasks)
        logging.info(f"Queued tasks: {queued_tasks}")

        return queued_tasks

    @property
    def running(self) -> set[TaskInstanceKey]:
        """Return running tasks from local and resource executor."""
        return self.local_executor.running.union(self.resource_executor.running)

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
    ) -> LocalExecutor | ResourceExecutor:
        logging.info(f"Routing to queue: {simple_task_instance.queue}")
        if simple_task_instance.queue == self.RADICAL_QUEUE:
            return self.resource_executor
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
        """Queues task instance via local or resource executor."""
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
        logging.info("Starting local and resource Executor")
        self.local_executor.start()
        self.resource_executor.start()

    def end(self) -> None:
        logging.info("Ending local and resource Executor")
        self.local_executor.end()
        self.resource_executor.end()

    def has_task(self, task_instance: TaskInstance) -> bool:
        """Checks if a task is either queued or running in either local or resource executor."""
        return self.local_executor.has_task(
            task_instance
        ) or self.resource_executor.has_task(task_instance)

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs in local and resource executor."""
        self.local_executor.heartbeat()
        self.resource_executor.heartbeat()

    def get_event_buffer(
        self, dag_ids: list[str] | None = None
    ) -> dict[TaskInstanceKey, EventBufferValueType]:
        """Return and flush the event buffer from local and resource executor."""
        cleared_events_from_local = self.local_executor.get_event_buffer(dag_ids)
        cleared_events_from_radical = self.resource_executor.get_event_buffer(dag_ids)

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
            *self.resource_executor.try_adopt_task_instances(radical_tis),
        ]