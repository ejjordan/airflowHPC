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
import os
import subprocess
from multiprocessing import Manager, Process
from queue import Empty
from typing import TYPE_CHECKING, Any, Optional, Tuple, List

from radical.pilot import Slot
from setproctitle import getproctitle, setproctitle

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance

from airflowHPC.hooks.gpu import GPUHook

if TYPE_CHECKING:
    from multiprocessing.managers import SyncManager
    from queue import Queue

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceStateType
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
    ExecutorWorkType = Tuple[
        Optional[TaskInstanceKey], Optional[CommandType], Optional[list[int]]
    ]


class ResourceWorker(Process, LoggingMixin):
    """
    ResourceWorkerBase implementation to run airflow commands.

    Executes the given command and puts the result into a result queue when done, terminating execution.

    :param result_queue: the queue to store result state
    """

    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
        gpu_env_var_name: str | None = None,
    ):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.task_queue = task_queue
        self.result_queue: Queue[TaskInstanceStateType] = result_queue
        self.gpu_env_var_name = gpu_env_var_name

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- ResourceExecutor")
        return super().run()

    def execute_work(
        self, key: TaskInstanceKey, command: CommandType, gpu_ids: list[int]
    ) -> None:
        """
        Execute command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- ResourceExecutor: {command}")
        env = os.environ.copy()
        visible_devices = ",".join(map(str, gpu_ids))
        env.update({self.gpu_env_var_name: visible_devices})
        state = self._execute_work_in_subprocess(command, env)
        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- ResourceExecutor")

    def _execute_work_in_subprocess(
        self, command: CommandType, env
    ) -> TaskInstanceState:
        try:
            subprocess.check_call(command, close_fds=True, env=env)
            return TaskInstanceState.SUCCESS
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to execute task %s.", e)
            return TaskInstanceState.FAILED

    def do_work(self) -> None:
        while True:
            try:
                key, command, gpu_ids = self.task_queue.get()
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
                self.execute_work(key=key, command=command, gpu_ids=gpu_ids)
            finally:
                self.task_queue.task_done()


class ResourceExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = True

    serve_logs: bool = True

    def __init__(self):
        self.gpu_hook = GPUHook()
        super().__init__(
            parallelism=self.gpu_hook.num_nodes * self.gpu_hook.cores_per_node
        )
        if self.parallelism < 0:
            raise AirflowException("parallelism must be bigger than or equal to 0")
        self.manager: SyncManager | None = None
        self.result_queue: Queue[TaskInstanceStateType] | None = None
        self.task_queue: Queue[ExecutorWorkType] | None = None
        self.workers: list[ResourceWorker] = []
        self.slots_dict: dict[TaskInstanceKey, Slot] = {}

    def start(self) -> None:
        """Start the executor."""
        old_proctitle = getproctitle()
        setproctitle("airflow executor -- ResourceExecutor")
        self.manager = Manager()
        setproctitle(old_proctitle)
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_active = 0

        self.task_queue = self.manager.Queue()
        self.workers = [
            ResourceWorker(
                self.task_queue, self.result_queue, self.gpu_hook.gpu_env_var_name
            )
            for _ in range(self.parallelism)
        ]

        for worker in self.workers:
            worker.start()

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: str | None = None,
    ):
        """Queues command to task."""
        if task_instance.key not in self.queued_tasks:
            self.log.info("Adding to queue: %s", command)
            if task_instance.executor_config:
                self.gpu_hook.set_task_resources(
                    task_instance_key=task_instance.key,
                    num_cores=task_instance.executor_config["mpi_ranks"],
                    num_gpus=task_instance.executor_config["gpus"],
                )
            else:
                self.log.info(
                    f"Setting task resources to 1 core and 0 gpus for task {task_instance.key}"
                )
                self.gpu_hook.set_task_resources(
                    task_instance_key=task_instance.key,
                    num_cores=1,
                    num_gpus=0,
                )
            self.queued_tasks[task_instance.key] = (
                command,
                priority,
                queue,
                task_instance,
            )

        else:
            self.log.error("could not queue task %s", task_instance.key)

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Execute asynchronously."""

        self.validate_airflow_tasks_run_command(command)

        if TYPE_CHECKING:
            assert self.task_queue

        gpu_ids = self.gpu_hook.get_gpu_ids(key)
        self.task_queue.put((key, command, gpu_ids))

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Initiate async execution of the queued tasks, up to the number of available slots.

        :param open_slots: Number of open slots
        """
        sorted_queue = self.order_queued_tasks_by_priority()
        task_tuples = []

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, priority, queue, ti) = sorted_queue.pop(0)

            # If a task makes it here but is still understood by the executor
            # to be running, it generally means that the task has been killed
            # externally and not yet been marked as failed.
            #
            # However, when a task is deferred, there is also a possibility of
            # a race condition where a task might be scheduled again during
            # trigger processing, even before we are able to register that the
            # deferred task has completed. In this case and for this reason,
            # we make a small number of attempts to see if the task has been
            # removed from the running set in the meantime.
            if key in self.running:
                attempt = self.attempts[key]
                if attempt.can_try_again():
                    # if it hasn't been much time since first check, let it be checked again next time
                    self.log.info(
                        "queued but still running; attempt=%s task=%s",
                        attempt.total_tries,
                        key,
                    )
                    continue
                # Otherwise, we give up and remove the task from the queue.
                self.log.error(
                    "could not queue task %s (still running after %d attempts)",
                    key,
                    attempt.total_tries,
                )
                del self.attempts[key]
                del self.queued_tasks[key]
            else:
                found_slots = self.gpu_hook.assign_task_resources(key)
                if not found_slots:
                    sorted_queue.append(
                        (key, (command, priority, queue, ti.executor_config))
                    )
                    break

                if key in self.attempts:
                    del self.attempts[key]
                task_tuples.append((key, command, queue, ti))

        if task_tuples:
            self._process_tasks(task_tuples)

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(Empty):
            while True:
                key, state = self.result_queue.get_nowait()
                try:
                    self.change_state(key=key, state=state)
                    if state in {TaskInstanceState.SUCCESS, TaskInstanceState.FAILED}:
                        self.gpu_hook.release_task_resources(key)
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
            self.task_queue.put((None, None, None))

        # Wait for commands to finish
        self.task_queue.join()
        self.sync()
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
