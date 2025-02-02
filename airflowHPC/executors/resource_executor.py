from __future__ import annotations

import contextlib
import os
import subprocess
from multiprocessing import Manager, Process
from queue import Empty
from time import sleep
from typing import TYPE_CHECKING, Any, List, Optional, Tuple
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
            f"Task {key.dag_id}.{key.task_id}.{key.map_index} finished with state {state} on {self.name}"
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
        self.log.info(f"{self.__class__.__name__} parallelism: {self.parallelism}")
        self.manager = Manager()
        self.result_queue: Queue[TaskInstanceStateType] = self.manager.Queue()
        self.task_queue: Queue[ExecutorWorkType] = self.manager.Queue()
        self.workers: list[ResourceWorker] = []

    def start(self) -> None:
        """Start the executor."""
        old_proctitle = getproctitle()
        setproctitle(f"airflow executor -- {self.__class__.__name__}")
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
                    num_gpus = self.slurm_hook.task_resource_requests[
                        task_instance.key
                    ].num_gpus
                    if num_gpus > 0:
                        msg += f"and {num_gpus} GPU(s) "
                        msg += f"with occupation {self.slurm_hook.task_resource_requests[task_instance.key].gpu_occupation} "
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

        rank_ids = self.slurm_hook.get_rank_ids(key)
        gpu_ids = self.slurm_hook.get_gpu_ids(key)
        hostname = self.slurm_hook.get_hostname(key)
        self.task_queue.put((key, command, rank_ids, gpu_ids, hostname))

    def trigger_tasks(self, ti_keys: List[TaskInstanceKey]) -> None:
        """
        Initiate async execution of the queued tasks.

        This executor sends the tasks to execute while the BaseExecutor sends the number of tasks.
        This is needed so that only tasks with available resources are sent to the workers.

        :param ti_keys: List of TaskInstanceKey to trigger
        """
        task_tuples = []

        for key in ti_keys:
            (command, _, queue, ti) = self.queued_tasks[key]
            if self.slurm_hook.assign_task_resources(key):
                if self.log.level == 10:  # DEBUG
                    msg = f"ALLOCATED task {key.task_id}.{key.map_index} using cores: "
                    msg += f"{self.slurm_hook.get_core_ids(key)} and rank IDs: "
                    msg += f"{self.slurm_hook.get_rank_ids(key)} on {self.slurm_hook.get_hostname(key)}"
                    num_gpus = self.slurm_hook.get_gpu_ids(key)
                    if num_gpus:
                        msg += f" and GPU IDs: {num_gpus}"
                    self.log.debug(f"\033[1;93m{msg}\033[0m")
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
                    if key in self.attempts:
                        del self.attempts[key]
                    task_tuples.append((key, command, queue, ti.executor_config))
            else:
                self.log.info(
                    f"\033[1;91mTask {key.task_id}.{key.map_index} could not be executed due to lack of resources\033[0m"
                )

        if task_tuples:
            self._process_tasks(task_tuples)

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""
        # Free any resources that are no longer being used
        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

        sorted_queue = self.order_queued_tasks_by_priority()
        ti_keys, slots = self.slurm_hook.find_available_slots(
            [task for task, _ in sorted_queue]
        )
        open_slots = len(slots)
        if open_slots > 0:
            self.log.debug(
                f"Queued tasks: {[(task.dag_id, task.task_id, task.map_index) for task in self.queued_tasks]}"
            )
            self.log.debug(
                f"Running tasks: {[(task.dag_id, task.task_id, task.map_index) for task in self.running]}"
            )
            self.log.debug(
                f"SLOTS: {[[ro.index for ro in slot.cores] for slot in slots]}"
            )
            self.log.debug(
                f"\033[1;95mSlot: {[(slot.hostname, [core.index for core in slot.cores]) for slot in slots]}\033[0m"
            )

        num_running_tasks = len(self.running)
        num_queued_tasks = len(self.queued_tasks)

        if num_running_tasks > 0:
            self.log.debug("%s running task instances", num_running_tasks)
            self.log.debug(f"Free cores: {self.slurm_hook.free_cores_list()}")
        if num_queued_tasks > 0:
            self.log.debug("%s in queue", num_queued_tasks)
        if open_slots > 0:
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

        self.trigger_tasks(ti_keys)

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
                            f"\033[1;92mFREED task {key.task_id}.{key.map_index} using cores: {core_ids}\033[0m"
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
            f"Shutting down {self.__class__.__name__}"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )
        for _ in self.workers:
            self.task_queue.put((None, None, None, None, None))

        # Wait for commands to finish
        sleep(1)  # give results a little time to come in
        self.sync()
        if self.result_queue.qsize() > 0:
            self.log.info(
                f"{self.__class__.__name__} shut down with {self.result_queue.qsize()} results in queue"
            )
        if self.task_queue.qsize() > 0:
            self.log.info(
                f"{self.__class__.__name__} shut down with {self.task_queue.qsize()} tasks in queue"
            )
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
