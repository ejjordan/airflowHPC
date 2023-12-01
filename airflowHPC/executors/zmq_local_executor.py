from __future__ import annotations

import contextlib
import logging
import os
import subprocess
from multiprocessing import Manager, Process
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Optional, Tuple

from setproctitle import getproctitle, setproctitle

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceStateType
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
    ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]

    # Task tuple to send to be executed
    TaskTuple = Tuple[TaskInstanceKey, CommandType, Optional[str], Optional[Any]]


class LocalWorker(Process, LoggingMixin):
    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
    ):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.result_queue: Queue[TaskInstanceStateType] = result_queue
        self.task_queue = task_queue

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- {self.__class__.__name__}")
        return super().run()

    def execute_work(self, key: TaskInstanceKey, command: CommandType) -> None:
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- {self.__class__.__name__}: {command}")
        if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
            state = self._execute_work_in_subprocess(command)
        else:
            state = self._execute_work_in_fork(command)

        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle(f"airflow worker -- {self.__class__.__name__}")

    def _execute_work_in_subprocess(self, command: CommandType) -> TaskInstanceState:
        try:
            subprocess.check_call(command, close_fds=True)
            return TaskInstanceState.SUCCESS
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to execute task %s.", e)
            return TaskInstanceState.FAILED

    def _execute_work_in_fork(self, command: CommandType) -> TaskInstanceState:
        pid = os.fork()
        if pid:
            # In parent, wait for the child
            pid, ret = os.waitpid(pid, 0)
            return TaskInstanceState.SUCCESS if ret == 0 else TaskInstanceState.FAILED

        from airflow.sentry import Sentry

        ret = 1
        try:
            import signal

            from airflow.cli.cli_parser import get_parser

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGUSR2, signal.SIG_DFL)

            parser = get_parser()
            # [1:] - remove "airflow" from the start of the command
            args = parser.parse_args(command[1:])
            args.shut_down_logging = False

            setproctitle(f"airflow task supervisor: {command}")

            args.func(args)
            ret = 0
            return TaskInstanceState.SUCCESS
        except Exception as e:
            self.log.exception("Failed to execute task %s.", e)
            return TaskInstanceState.FAILED
        finally:
            Sentry.flush()
            logging.shutdown()
            os._exit(ret)

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


class ZmqLocalExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = False

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        if self.parallelism <= 1:
            raise AirflowException(f"{self.__class__.__name__} parallelism must be > 1")
        self.manager: SyncManager | None = None
        self.result_queue: Queue[TaskInstanceStateType] | None = None
        self.workers: list[LocalWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0
        self.queue: Queue[ExecutorWorkType] | None = None

    def start(self) -> None:
        """Start the executor."""
        old_proctitle = getproctitle()
        setproctitle(f"airflow executor -- {self.__class__.__name__}")
        self.manager = Manager()
        setproctitle(old_proctitle)
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0

        self.queue = self.manager.Queue()
        self.workers = [
            LocalWorker(self.queue, self.result_queue) for _ in range(self.parallelism)
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
        self.validate_airflow_tasks_run_command(command)

        self.queue.put((key, command))

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(Empty):
            while True:
                results = self.result_queue.get_nowait()
                try:
                    self.change_state(*results)
                finally:
                    self.result_queue.task_done()

    def end(self) -> None:
        self.log.info(
            f"Shutting down {self.__class__.__name__}"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )

        for _ in self.workers:
            self.queue.put((None, None))

            # Wait for commands to finish
            self.queue.join()
            self.sync()

        self.manager.shutdown()
