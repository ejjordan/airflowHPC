from __future__ import annotations

import queue
import contextlib
import sys
import time
import os
from typing import TYPE_CHECKING, Any, Optional, Tuple, Sequence

from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

import radical.pilot as rp
import radical.utils as ru

import logging


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


class RadicalExecutor(LocalExecutor):

    def __init__(self, parallelism: int = PARALLELISM):

        # FIXME: `parallelism` should be infinite, it is handled by RCT
        super().__init__(parallelism=parallelism)

        self.log.info(f"=== RadicalExecutor: __init__ {parallelism}")
        self._rct_session = None
        self._rct_pmgr = None
        self._rct_tmgr = None
        self._rct_pubsub = None
        self._rct_pub = None
        self._rct_sub = None


    def start(self) -> None:
        """Start the executor."""

        self._rct_session = rp.Session()
        self._rct_pmgr    = rp.PilotManager(session=self._rct_session)
        self._rct_tmgr    = rp.TaskManager(session=self._rct_session)

        self._rct_tmgr.register_callback(self._rct_state_cb)

        self._rct_pubsub = ru.zmq.PubSub('rct')
        self._rct_pubsub.start()
        time.sleep(1)  # let zmq settle

        self.log.info(f"======= SUB URL: {self._rct_pubsub.addr_sub}")
        self._rct_sub = ru.zmq.Subscriber(channel='rct', url=self._rct_pubsub.addr_sub)
        self._rct_sub.subscribe('request', cb=self._request_cb)

        self.log.info(f"======= PUB URL: {self._rct_pubsub.addr_pub}")
        self._rct_pub = ru.zmq.Publisher(channel='rct', url=self._rct_pubsub.addr_pub)

        # 'airflow tasks run' lives in the same environment as the executor
        # and gets the ZMQ endpoints passed as environment variables
        os.environ['RCT_PUB_URL'] = str(self._rct_pubsub.addr_pub)
        os.environ['RCT_SUB_URL'] = str(self._rct_pubsub.addr_sub)

        self.log.info("=== RadicalExecutor: start")

        # TODO: how to get pilot size and resource label from application level?
        #       probably best to run within an allocation...
        pd = rp.PilotDescription({"resource": "local.localhost",
                                  "cores"   : self.parallelism,
                                  "runtime" : 30}
                             )
        pilot = self._rct_pmgr.submit_pilots(pd)
        self._rct_tmgr.add_pilots(pilot)

        # rct is set up, zmq env is known - start the inherited local executor
        super().start()

        self.log.info('=== RadicalExecutor: start ok')

    def _rct_state_cb(self, task, state):
        tid = task.uid
        self.log.info(f"=== {tid}: {state}")
        self._rct_pub.put('update', {'task': task.as_dict()})

    def _request_cb(self, topic, msg):
        import pprint
        self.log.info("================ request: %s" % pprint.pformat(msg))
        task = self._rct_tmgr.submit_tasks(rp.TaskDescription(msg['td']))
        self.log.info("================ update: %s" % pprint.pformat(task))
        self._rct_pub.put('update', {'task': task.as_dict()})

    # def execute_async(
    #     self,
    #     key: TaskInstanceKey,
    #     command: CommandType,
    #     queue: str | None = None,
    #     executor_config: Any | None = None,
    # ) -> None:
    #     from airflow.utils.cli import get_dag
    #     import os

    #     self.log.info(f"=== execute_async {key}: {command}")

    #     dag = get_dag(dag_id=key.dag_id, subdir=os.path.join("dags", key.dag_id))
    #     task = dag.get_task(key.task_id)

    #     rp_out_paths = list()
    #     if hasattr(task, "op_kwargs"):
    #         for k, v in task.op_kwargs.get("output_files", {}).items():
    #             rp_out_paths.append(os.path.join(task.op_kwargs["output_dir"], v))

    #     self.validate_airflow_tasks_run_command(command)
    #     td = rp.TaskDescription()
    #     td.executable = command[0]
    #     td.arguments = command[1:]
    #     td.metadata = {"key": key}
    #     td.named_env = self._rct_env_name
    #     td.output_staging = [
    #         {
    #             "source": f"task:///{out_path}",
    #             "target": f"client:///{out_path}",
    #             "action": rp.TRANSFER,
    #         }
    #         for out_path in rp_out_paths
    #     ]
    #     self.log.info(f"=== output_staging: {td.output_staging}")

    #     task = self._rct_tmgr.submit_tasks(td)

    #     self._rct_keys[task.uid] = key
    #     self.log.info(f"=== submitted task: {task}")

    # def sync(self) -> None:
    #     """Sync will get called periodically by the heartbeat method."""
    #     with contextlib.suppress(queue.Empty):
    #         while True:
    #             results = self._rct_results.get_nowait()
    #             try:
    #                 self.change_state(*results)
    #             finally:
    #                 self._rct_results.task_done()

    # def end(self) -> None:
    #     self.log.info(f"=== RadicalExecutor: end")
    #     self._rct_session.close()
