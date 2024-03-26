from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from typing import TYPE_CHECKING, Any, Optional, Tuple
from setproctitle import setproctitle
from airflow import settings
from airflow.utils.state import TaskInstanceState
import os
import logging
import subprocess
from radical.utils import which as ru_which

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


class MPIExecutor(BaseExecutor):
    """
    MPIExecutor is an executor that can distribute task execution in parallel
    using MPI. It is a subclass of the Base
    """

    def __init__(self, num_ranks: int = PARALLELISM):
        super().__init__(parallelism=num_ranks)
        self.result_queue: Queue[TaskInstanceStateType] | None = None
        self.ranks_active: int = 0
        self.ranks_total: int = num_ranks

    def start(self) -> None:
        self.mpi_cmd = ru_which(
            [
                "mpiexec",  # MPICH
                "mpirun",  # openmpi
            ]
        )

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        num_ranks = executor_config.get("ranks", 1)
        mpi_command = f"{self.mpi_cmd} -np {num_ranks} {command}"

        # Execute the command
        process = subprocess.Popen(mpi_command, shell=True)

    def execute_work(self, key: TaskInstanceKey, command: CommandType) -> None:
        """
        Execute command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- LocalExecutor: {command}")
        if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
            state = self._execute_work_in_subprocess(command)
        else:
            state = self._execute_work_in_fork(command)

        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- LocalExecutor")

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
