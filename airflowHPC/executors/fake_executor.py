from __future__ import annotations

import contextlib
import logging
import os
import subprocess
from abc import abstractmethod
from multiprocessing import Manager, Process
from queue import Empty
from typing import TYPE_CHECKING, Any, Optional, Tuple

from setproctitle import getproctitle, setproctitle

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from radical.utils import get_exception_trace
from mpi4py.futures import MPIPoolExecutor

if TYPE_CHECKING:
    from multiprocessing.managers import SyncManager
    from queue import Queue

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceStateType
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
    ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]


class QueuedFakeWorker(Process, LoggingMixin):
    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
    ):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.result_queue: Queue[TaskInstanceStateType] = result_queue
        self.task_queue = task_queue

    def do_work(self) -> None:
        while True:
            try:
                key, command = self.task_queue.get()
            except EOFError:
                self.log.info(
                    "Failed to read tasks from the task queue because the other "
                    "end has closed the connection. Terminating worker %s.",
                    self.name,
                )
                break
            try:
                if key is None or command is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key=key, command=command)
            finally:
                self.task_queue.task_done()

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- LocalExecutor")
        return super().run()

    def execute_work(self, key: TaskInstanceKey, command: CommandType) -> None:
        """
        Execute command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- FakeExecutor: {command}")
        state = self._execute_work_in_popen(command)

        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- FakeExecutor")

    def _execute_work_in_popen(self, command: CommandType) -> TaskInstanceState:
        try:
            subprocess.run(
                command,
                stdin=None,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                close_fds=True,
                shell=False,
            )
            return TaskInstanceState.SUCCESS
        except Exception as e:
            self.log.error("Failed to execute task %s.", e)
            return TaskInstanceState.FAILED


class FakeExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = False

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        if self.parallelism < 0:
            raise AirflowException("parallelism must be bigger than or equal to 0")

        self.manager = Manager()
        self.task_queue: Queue[ExecutorWorkType] = self.manager.Queue()
        self.result_queue: Queue[TaskInstanceStateType] = self.manager.Queue()

        self.workers: list[QueuedFakeWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0

    def start(self) -> None:
        """Start the executor."""
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0

        self.queue = self.manager.Queue()
        self.workers = [
            QueuedFakeWorker(self.queue, self.result_queue)
            for _ in range(self.parallelism)
        ]

        self.workers_used = len(self.workers)

        for worker in self.workers:
            worker.start()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Execute asynchronously."""

        self.validate_airflow_tasks_run_command(command)

        self.queue.put((key, command))

    def sync(self):
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(Empty):
            while True:
                results = self.result_queue.get_nowait()
                try:
                    self.change_state(*results)
                finally:
                    self.result_queue.task_done()

    def end(self):
        """
        End the executor.

        Sends the poison pill to all workers.
        """
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )
        for _ in self.workers:
            self.queue.put((None, None))

        # Wait for commands to finish
        self.queue.join()
        self.sync()
        self.manager.shutdown()

    def _flush_task_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.task_queue

        self.log.debug(
            "Executor shutting down, task_queue approximate size=%d",
            self.task_queue.qsize(),
        )
        with contextlib.suppress(Empty):
            while True:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning("Executor shutting down, will NOT run task=%s", task)
                self.task_queue.task_done()

    def _flush_result_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.result_queue

        self.log.debug(
            "Executor shutting down, result_queue approximate size=%d",
            self.result_queue.qsize(),
        )
        with contextlib.suppress(Empty):
            while True:
                results = self.result_queue.get_nowait()
                self.log.warning("Executor shutting down, flushing results=%s", results)
                try:
                    key, state = results
                    self.log.info(
                        "Changing state of %s to %s : resource_version=%d",
                        results,
                        state,
                    )
                    try:
                        self.change_state(key, state)
                    except Exception as e:
                        self.log.exception(
                            "Ignoring exception: %s when attempting to change state of %s to %s.",
                            e,
                            results,
                            state,
                        )
                finally:
                    self.result_queue.task_done()

    """
    def end(self) -> None:
        if TYPE_CHECKING:
            assert self.task_queue
            assert self.result_queue

        self.log.info("Shutting down Kubernetes executor")
        try:
            self.log.debug("Flushing task_queue...")
            self._flush_task_queue()
            self.log.debug("Flushing result_queue...")
            self._flush_result_queue()
            # Both queues should be empty...
            self.task_queue.join()
            self.result_queue.join()
        except ConnectionResetError:
            self.log.exception("Connection Reset error while flushing task_queue and result_queue.")
        self.manager.shutdown()
    """

    def terminate(self):
        """Terminate the executor is not doing anything."""
