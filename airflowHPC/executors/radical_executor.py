from __future__ import annotations

import time
import os
from typing import TYPE_CHECKING, Any, Optional, Tuple

from airflow.executors.base_executor import PARALLELISM
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.state import TaskInstanceState

import radical.pilot as rp
import radical.utils as ru


if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstance

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

        self.log.info(f"RadicalExecutor: __init__ {parallelism}")
        self._rct_session = None
        self._rct_pmgr = None
        self._rct_tmgr = None
        self._rct_pubsub = None
        self._rct_pub = None
        self._rct_sub = None

    def start(self) -> None:
        """Start the executor."""

        self._rct_session = rp.Session()
        self._rct_pmgr = rp.PilotManager(session=self._rct_session)
        self._rct_tmgr = rp.TaskManager(session=self._rct_session)

        self._rct_tmgr.register_callback(self._rct_state_cb)

        self._rct_pubsub = ru.zmq.PubSub("rct")
        self._rct_pubsub.start()
        time.sleep(1)  # let zmq settle

        self.log.info(f"SUB URL: {self._rct_pubsub.addr_sub}")
        self._rct_sub = ru.zmq.Subscriber(channel="rct", url=self._rct_pubsub.addr_sub)
        self._rct_sub.subscribe("request", cb=self._request_cb)

        self.log.info(f"PUB URL: {self._rct_pubsub.addr_pub}")
        self._rct_pub = ru.zmq.Publisher(channel="rct", url=self._rct_pubsub.addr_pub)

        # "airflow tasks run" lives in the same environment as the executor
        # and gets the ZMQ endpoints passed as environment variables
        os.environ["RCT_PUB_URL"] = str(self._rct_pubsub.addr_pub)
        os.environ["RCT_SUB_URL"] = str(self._rct_pubsub.addr_sub)

        self.log.info("RadicalExecutor: start")

        # TODO: how to get pilot size and resource label from application level?
        #       probably best to run within an allocation...
        pd = rp.PilotDescription({"resource": "local.localhost",
                                  "cores": self.parallelism,
                                  "runtime": 30})
        pilot = self._rct_pmgr.submit_pilots(pd)
        self._rct_tmgr.add_pilots(pilot)

        # keep track of used slots
        # NOTE: This considers only single-core tasks which do not use GPUs.
        #       Otherwise slot counting of this manner does not make much sense,
        #       really, as a slot may have very different shapes for different
        #       tasks...
        self._free_slots = self.parallelism

        # rct is set up, zmq env is known - start the inherited local executor
        super().start()

        self.log.info("RadicalExecutor: start ok")

    def end(self) -> None:
        self.log.info("RadicalExecutor: end")
        self._rct_session.close()
        super().end()

    def _rct_state_cb(self, task, state):
        tid = task.uid
        op_id = task.metadata["op_id"]
        self.log.info(f"{tid}: {state}")
        self._rct_pub.put("update", {"op_id": op_id, "task": task.as_dict()})

        if state in rp.FINAL:
            self._free_slots += 1
            # self.change_state(
            #     key=tid,
            #     state=TaskInstanceState.SUCCESS if state == rp.DONE else TaskInstanceState.FAILED,
            # )

    def _request_cb(self, topic, msg):
        import pprint
        self.log.info("request: %s" % pprint.pformat(msg))
        op_id = msg["op_id"]
        td = msg["td"]
        td["metadata"] = {"op_id": op_id}
        task = self._rct_tmgr.submit_tasks(rp.TaskDescription(td))
        self.log.info("update: %s" % pprint.pformat(task))
        self._rct_pub.put("update", {"op_id": op_id, "task": task.as_dict()})
        self._free_slots -= 1
        # self.change_state(
        #     key=task.uid,
        #     state=TaskInstanceState.RUNNING,
        # )

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
                try:
                    if self._free_slots < 1:
                        self.log.debug(f"No available resources for task: {key}.")
                        sorted_queue.append(
                            (key, (command, priority, queue, ti.executor_config))
                        )
                        break
                except:
                    self.log.error(f"No viable resource assignment for task: {key}.")
                    del self.queued_tasks[key]
                    self.change_state(
                        key=key,
                        state=TaskInstanceState.FAILED,
                        info=f"No viable resource assignment for executor_config {ti.executor_config}",
                    )
                    break

                if key in self.attempts:
                    del self.attempts[key]
                task_tuples.append((key, command, queue, ti))

        if task_tuples:
            self._process_tasks(task_tuples)
