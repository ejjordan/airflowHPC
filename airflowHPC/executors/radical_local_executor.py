from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Optional, Tuple

from airflow.executors.base_executor import PARALLELISM, BaseExecutor

import radical.utils as ru
import radical.pilot as rp


if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance     import TaskInstanceStateType
    from airflow.models.taskinstancekey  import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die
    # instantly.
    ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]

    # Task tuple to send to be executed
    TaskTuple = Tuple[TaskInstanceKey, CommandType, Optional[str], Optional[Any]]

class RadicalLocalExecutor(BaseExecutor):
    is_local: bool = True
    supports_pickling: bool = False

    serve_logs: bool = True

    def __init__(self, parallelism: int = PARALLELISM):

        super().__init__(parallelism=parallelism)
        self.log.info(f"=== RadicalLocalExecutor: __init__ {parallelism}")


    def start(self) -> None:
        """Start the executor."""
        self._rp_session = rp.Session()
        self._rp_log     = self._rp_session._log
        self._rp_pmgr    = rp.PilotManager(session=self._rp_session)
        self._rp_tmgr    = rp.TaskManager(session=self._rp_session)

        self._rp_log.debug(f"=== RadicalLocalExecutor: start")
        pd = rp.PilotDescription({'resource': 'local.localhost',
                                  'cores'   : self.parallelism,
                                  'runtime' : 30})
        pilot = self._rp_pmgr.submit_pilots(pd)
        self._rp_tmgr.add_pilots(pilot)

        def state_cb(task, state):
            self._rp_log.debug('=== task state %s: %s' % (task.uid, state))

        self._rp_tmgr.register_callback(state_cb)


    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        self._rp_log.debug(f"=== execute_async {key}: {command}")
        self.validate_airflow_tasks_run_command(command)

        td = rp.TaskDescription()
        td.executable = command[0]
        td.arguments  = command[1:]

        task = self._rp_tmgr.submit_tasks(td)
        self._rp_log.debug(f"=== submitted task: {task}")


    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        # FIXME
        return


    def end(self) -> None:
        self._rp_log.debug(f"=== RadicalLocalExecutor: end")
        self._rp_session.close()

