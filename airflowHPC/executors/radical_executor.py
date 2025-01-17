from __future__ import annotations

import contextlib
import os
import time
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

from airflowHPC.operators import is_resource_rct_operator

import radical.pilot as rp
import radical.utils as ru


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
    ]

PARALLELISM = int(os.environ.get("RCT_PARALLELISM", 64))


class ResourceWorker(Process, LoggingMixin):
    """
    ResourceWorkerBase implementation to run airflow commands.

    Executes the given command and puts the result into a result queue when done, terminating execution.

    :param task_queue: the queue to get the tasks from
    :param result_queue: the queue to put the results in
    """

    def __init__(
        self,
        task_queue: Queue[ExecutorWorkType],
        result_queue: Queue[TaskInstanceStateType],
    ):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.task_queue = task_queue
        self.result_queue: Queue[TaskInstanceStateType] = result_queue

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle(f"airflow worker -- {self.__class__.__name__}")
        return super().run()

    def execute_work(
        self,
        key: TaskInstanceKey,
        command: CommandType,
    ) -> None:
        """
        Execute command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- {self.__class__.__name__}: {command}")
        env = os.environ.copy()
        state = self._execute_work_in_subprocess(command, env)
        self.result_queue.put((key, state))
        self.log.info(
            f"Task {key.task_id}.{key.map_index} finished with state {state} on {self.name}"
        )
        # Remove the command since the worker is done executing the task
        setproctitle(f"airflow worker -- {self.__class__.__name__}")

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
                key, command = self.task_queue.get()
            except EOFError:
                self.log.info(
                    "Failed to read tasks from the task queue because the other "
                    "end has closed the connection. Terminating worker %s.",
                    self.name,
                )
                break
            try:
                if key is None and command is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key=key, command=command)

            finally:
                self.task_queue.task_done()


class RadicalExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = True

    serve_logs: bool = True

    def __init__(self):
        self.log.info(f"{self.__class__.__name__}: __init__ {PARALLELISM}")

        super().__init__(parallelism=PARALLELISM)

        if self.parallelism < 0:
            raise AirflowException("parallelism must be >= 0")

        self.manager = Manager()
        self.result_queue: Queue[TaskInstanceStateType] = self.manager.Queue()
        self.task_queue: Queue[ExecutorWorkType] = self.manager.Queue()
        self.workers: list[ResourceWorker] = []

        self._rct_session = None
        self._rct_pmgr = None
        self._rct_tmgr = None
        self._rct_server = None
        self._rct_tasks = dict()

    def _submit_task(self, task_description):
        td = rp.TaskDescription(task_description)
        task = self._rct_tmgr.submit_tasks(td)
        self._rct_tasks[task.uid] = task
        return task.uid

    def _check_task(self, uid):
        task = self._rct_tasks.get(uid)
        if not task:
            return None, None
        return task.state, task.exit_code

    def start(self) -> None:
        """Start the executor."""

        self.log.info("{self.__class__.__name__}: start")
        self.log.warning(
            "\033[1;91mThis executor is experimental. It may result in large numbers of zombie processes.\033[0m"
        )

        self._rct_session = rp.Session()
        self._rct_pmgr = rp.PilotManager(session=self._rct_session)
        self._rct_tmgr = rp.TaskManager(session=self._rct_session)

        self._rct_server = ru.zmq.Server()
        self._rct_server.register_request("submit", self._submit_task)
        self._rct_server.register_request("check", self._check_task)
        self._rct_server.start()
        time.sleep(0.1)  # let zmq settle

        # "airflow tasks run" lives in the same environment as the executor
        # and gets the ZMQ endpoints passed as environment variables
        os.environ["RCT_SERVER_URL"] = str(self._rct_server.addr)

        pilot_json = os.environ.get("RCT_PILOT_CFG")
        if pilot_json:
            self.log.debug("pilot_json: %s" % pilot_json)
            pd_dict = ru.read_json(pilot_json)

        else:
            self.log.debug("pilot_resource: localhost")
            pd_dict = {"resource": "local.localhost", "nodes": 128, "runtime": 1440}

        self.log.debug("pilot_description: %s" % pd_dict)

        pd = rp.PilotDescription(pd_dict)
        pilot = self._rct_pmgr.submit_pilots(pd)

        # let tasks run in the resource's native environment
        pilot.prepare_env("bs0", {"type": "shell"})
        self._rct_tmgr.add_pilots(pilot)

        # wait for the pilot to become active (but don't stall on errors)
        pilot.wait(rp.FINAL + [rp.PMGR_ACTIVE])
        self.log.debug("pilot state: %s" % pilot.state)
        assert pilot.state == rp.PMGR_ACTIVE

        # rct is set up, zmq env is known - start the inherited local executor
        super().start()

        self.log.info(f"{self.__class__.__name__}: start ok")

        # pilot is up, also run Joe's executor workers.
        old_proctitle = getproctitle()
        setproctitle(f"airflow executor -- {self.__class__.__name__}")
        setproctitle(old_proctitle)
        self.workers = []

        self.workers = [
            ResourceWorker(
                self.task_queue,
                self.result_queue,
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
            if is_resource_rct_operator(task_instance.operator_name):
                assert task_instance.executor_config
                assert "mpi_ranks" in task_instance.executor_config

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

        self.task_queue.put((key, command))

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""

        # RP always has open slots
        open_slots = PARALLELISM

        self.log.debug(
            f"Queued tasks: {[(task.task_id, task.map_index) for task in self.queued_tasks]}"
        )
        self.log.debug(
            f"Running tasks: {[(task.task_id, task.map_index) for task in self.running]}"
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

        self.trigger_tasks(open_slots)

        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with contextlib.suppress(Empty):
            while True:
                key, state = self.result_queue.get_nowait()
                try:
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
            self.task_queue.put((None, None))

        # Wait for commands to finish
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

        self._rct_session.close()

    def terminate(self):
        """Terminate the executor is not doing anything."""
