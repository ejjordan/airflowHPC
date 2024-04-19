#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
ResourceExecutor.

.. seealso::
    For more information on how the ResourceExecutor works, take a look at the guide:
    :ref:`executor:ResourceExecutor`
"""
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


class ResourceWorkerBase(Process, LoggingMixin):
    """
    ResourceWorkerBase implementation to run airflow commands.

    Executes the given command and puts the result into a result queue when done, terminating execution.

    :param result_queue: the queue to store result state
    """

    def __init__(self, result_queue: Queue[TaskInstanceStateType]):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.result_queue: Queue[TaskInstanceStateType] = result_queue

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- ResourceExecutor")
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
        setproctitle(f"airflow worker -- ResourceExecutor: {command}")
        if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
            state = self._execute_work_in_subprocess(command)
        else:
            state = self._execute_work_in_fork(command)

        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- ResourceExecutor")

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

    @abstractmethod
    def do_work(self):
        """Execute tasks; called in the subprocess."""
        raise NotImplementedError()


class ResourceWorker(ResourceWorkerBase):
    """
    Resource worker that executes the task.

    :param result_queue: queue where results of the tasks are put.
    :param key: key identifying task instance
    :param command: Command to execute
    """

    def __init__(
        self,
        result_queue: Queue[TaskInstanceStateType],
        key: TaskInstanceKey,
        command: CommandType,
    ):
        super().__init__(result_queue)
        self.key: TaskInstanceKey = key
        self.command: CommandType = command

    def do_work(self) -> None:
        self.execute_work(key=self.key, command=self.command)


class QueuedResourceWorker(ResourceWorkerBase):
    """
    ResourceWorker implementation that is waiting for tasks from a queue.

    Will continue executing commands as they become available in the queue.
    It will terminate execution once the poison token is found.

    :param task_queue: queue from which worker reads tasks
    :param result_queue: queue where worker puts results after finishing tasks
    """

    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
    ):
        super().__init__(result_queue=result_queue)
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


class ResourceExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = False

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        if self.parallelism < 0:
            raise AirflowException("parallelism must be bigger than or equal to 0")
        self.manager: SyncManager | None = None
        self.result_queue: Queue[TaskInstanceStateType] | None = None
        self.task_queue: Queue[ExecutorWorkType] | None = None
        self.workers: list[QueuedResourceWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0

    def start(self) -> None:
        """Start the executor."""
        old_proctitle = getproctitle()
        setproctitle("airflow executor -- ResourceExecutor")
        self.manager = Manager()
        setproctitle(old_proctitle)
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0

        self.task_queue = self.manager.Queue()
        self.workers = [
            QueuedResourceWorker(self.task_queue, self.result_queue)
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
        if TYPE_CHECKING:
            assert self.impl

        self.validate_airflow_tasks_run_command(command)

        if TYPE_CHECKING:
            assert self.task_queue

        self.task_queue.put((key, command))

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
        """End the executor."""
        if TYPE_CHECKING:
            assert self.manager

        self.log.info(
            "Shutting down ResourceExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )
        for _ in self.workers:
            self.task_queue.put((None, None))

        # Wait for commands to finish
        self.task_queue.join()
        self.sync()
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
