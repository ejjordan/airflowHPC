from __future__ import annotations

import contextlib
import os
import subprocess
from multiprocessing import Manager, Process
from queue import Empty
from typing import TYPE_CHECKING, Any, Optional, Tuple
from setproctitle import getproctitle, setproctitle

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats

from airflowHPC.hooks.slurm import SlurmHook
from airflowHPC.operators import is_resource_operator

if TYPE_CHECKING:
    from queue import Queue

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceStateType
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
    ExecutorWorkType = Tuple[
        Optional[TaskInstanceKey],
        Optional[CommandType],
        Optional[list[int]],
        Optional[list[int]],
        Optional[str],
    ]


class ResourceWorker(Process, LoggingMixin):
    """
    ResourceWorkerBase implementation to run airflow commands.

    Executes the given command and puts the result into a result queue when done, terminating execution.

    :param task_queue: the queue to get the tasks from
    :param result_queue: the queue to put the results in
    :param gpu_env_var_name: the name of the environment variable to set the GPU IDs
    :param hostname_env_var_name: the name of the environment variable to set the hostname
    """

    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
        gpu_env_var_name: str | None = None,
        hostname_env_var_name: str | None = None,
    ):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.task_queue = task_queue
        self.result_queue: Queue[TaskInstanceStateType] = result_queue
        self.gpu_env_var_name = gpu_env_var_name
        self.hostname_env_var_name = hostname_env_var_name

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- ResourceExecutor")
        return super().run()

    def execute_work(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        core_ids: list[int],
        gpu_ids: list[int],
        hostname: str,
    ) -> None:
        """
        Execute command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        :param core_ids: the core IDs to use
        :param gpu_ids: the GPU IDs to use
        :param hostname: the hostname of the node
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- ResourceExecutor: {command}")
        env = os.environ.copy()
        if core_ids:
            self.log.debug(f"Using cores: {core_ids}")
            env.update({"CORE_IDS": f"{','.join(map(str, core_ids))}"})
        if gpu_ids:
            visible_devices = ",".join(map(str, gpu_ids))
            self.log.debug(f"Setting {self.gpu_env_var_name} to {visible_devices}")
            env.update({self.gpu_env_var_name: visible_devices})
        if hostname:
            self.log.debug(f"Setting {self.hostname_env_var_name} to {hostname}")
            env.update({self.hostname_env_var_name: hostname})
        state = self._execute_work_in_subprocess(command, env)
        self.result_queue.put((key, state))
        self.log.info(
            f"Task {key.task_id}.{key.map_index} finished with state {state} on {self.name}"
        )
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- ResourceExecutor")

    def _execute_work_in_subprocess(
        self, command: CommandType, env
    ) -> TaskInstanceState:
        try:
            subprocess.run(
                command,
                stdin=None,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                close_fds=True,
                shell=False,
                env=env,
            )
            return TaskInstanceState.SUCCESS
        except Exception as e:
            self.log.error("Failed to execute task %s.", e)
            return TaskInstanceState.FAILED

    def do_work(self) -> None:
        while True:
            try:
                key, command, core_ids, gpu_ids, hostname = self.task_queue.get()
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
                self.execute_work(
                    key=key,
                    command=command,
                    core_ids=core_ids,
                    gpu_ids=gpu_ids,
                    hostname=hostname,
                )
            finally:
                self.task_queue.task_done()


class ResourceExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = True

    serve_logs: bool = True

    def __init__(self):
        self.slurm_hook = SlurmHook()
        super().__init__(
            parallelism=self.slurm_hook.num_nodes * self.slurm_hook.tasks_per_node
        )
        if self.parallelism < 0:
            raise AirflowException("parallelism must be bigger than or equal to 0")
        self.manager = Manager()
        self.result_queue: Queue[TaskInstanceStateType] = self.manager.Queue()
        self.task_queue: Queue[ExecutorWorkType] = self.manager.Queue()
        self.workers: list[ResourceWorker] = []

    def start(self) -> None:
        """Start the executor."""
        old_proctitle = getproctitle()
        setproctitle("airflow executor -- ResourceExecutor")
        setproctitle(old_proctitle)
        self.workers = []
        self.workers_active = 0

        self.workers = [
            ResourceWorker(
                self.task_queue,
                self.result_queue,
                self.slurm_hook.gpu_env_var_name,
                self.slurm_hook.hostname_env_var_name,
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
            self.log.info(f"Adding to queue: {command}")
            if is_resource_operator(task_instance.operator_name):
                assert task_instance.executor_config
                assert "mpi_ranks" in task_instance.executor_config
                try:
                    self.slurm_hook.set_task_resources(
                        task_instance_key=task_instance.key,
                        num_ranks=task_instance.executor_config["mpi_ranks"],
                        num_threads=task_instance.executor_config.get(
                            "cpus_per_task", 1
                        ),
                        num_gpus=task_instance.executor_config.get("gpus", 0),
                    )
                    msg = f"Setting task resources to {self.slurm_hook.task_resource_requests[task_instance.key].num_ranks} "
                    msg += f"MPI ranks, {self.slurm_hook.task_resource_requests[task_instance.key].num_threads} threads, "
                    msg += f"and {self.slurm_hook.task_resource_requests[task_instance.key].num_gpus} GPU(s) "
                    msg += f"for task {task_instance.key.task_id}.{task_instance.key.map_index}"
                    self.log.info(msg)
                except ValueError:
                    self.log.error(
                        f"Failed to set task resources for task {task_instance.key.task_id}.{task_instance.key.map_index}"
                    )
                    self.change_state(
                        key=task_instance.key,
                        state=TaskInstanceState.FAILED,
                        info=f"No viable resource assignment for task: {task_instance.key.task_id}.{task_instance.key.map_index}",
                    )
                    return
            else:
                # No need to catch exceptions here, as the task will be queued with default resources
                self.log.info(
                    f"Setting task resources to 1 core and 0 gpus for task {task_instance.key.task_id}.{task_instance.key.map_index}"
                )
                self.slurm_hook.set_task_resources(
                    task_instance_key=task_instance.key,
                    num_ranks=1,
                    num_threads=1,
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

        self.slurm_hook.assign_task_resources(key)
        try:
            core_ids = self.slurm_hook.get_core_ids(key)
            rank_ids = self.slurm_hook.get_rank_ids(key)
            gpu_ids = self.slurm_hook.get_gpu_ids(key)
            hostname = self.slurm_hook.get_hostname(key)
            self.log.debug(
                f"ALLOCATED task {key.task_id}.{key.map_index} using cores: {core_ids} and rank IDs: {rank_ids}"
            )
            self.task_queue.put((key, command, rank_ids, gpu_ids, hostname))
        except RuntimeError:
            self.log.error(
                f"Failed to allocate resources for task {key.task_id}.{key.map_index}"
            )
            self.change_state(
                key=key,
                state=TaskInstanceState.FAILED,
                info=f"No viable resource assignment for task: {key.task_id}.{key.map_index}",
            )


    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""
        slots = self.slurm_hook.find_available_slots(
            [task for task in self.queued_tasks]
        )
        open_slots = len(slots)
        if open_slots > 0:
            self.log.debug(
                f"Queued tasks: {[(task.task_id, task.map_index) for task in self.queued_tasks]}"
            )
            self.log.debug(
                f"Running tasks: {[(task.task_id, task.map_index) for task in self.running]}"
            )
            self.log.debug(
                f"SLOTS: {[[ro.index for ro in slot.cores] for slot in slots]}"
            )

        num_running_tasks = len(self.running)
        num_queued_tasks = len(self.queued_tasks)

        self.log.debug("%s running task instances", num_running_tasks)
        self.log.debug("%s in queue", num_queued_tasks)
        self.log.debug("%s open slots", open_slots)

        Stats.gauge(
            "executor.open_slots",
            value=open_slots,
            tags={"status": "open", "name": self.__class__.__name__},
        )
        Stats.gauge(
            "executor.queued_tasks",
            value=num_queued_tasks,
            tags={"status": "queued", "name": self.__class__.__name__},
        )
        Stats.gauge(
            "executor.running_tasks",
            value=num_running_tasks,
            tags={"status": "running", "name": self.__class__.__name__},
        )

        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

        self.trigger_tasks(open_slots)

        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(Empty):
            while True:
                key, state = self.result_queue.get_nowait()
                try:
                    if state in {TaskInstanceState.SUCCESS, TaskInstanceState.FAILED}:
                        core_ids = self.slurm_hook.get_core_ids(key)
                        self.log.debug(
                            f"FREED task {key.task_id}.{key.map_index} using cores: {core_ids}"
                        )
                        # TODO: The slurm hook should be connected to a DB backend so that allocate and free
                        # can be called by the task execute method
                        self.slurm_hook.release_task_resources(key)
                    self.change_state(key, state)
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
            self.task_queue.put((None, None, None, None, None))

        # Wait for commands to finish
        self.task_queue.join()
        self.result_queue.join()
        self.sync()
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
