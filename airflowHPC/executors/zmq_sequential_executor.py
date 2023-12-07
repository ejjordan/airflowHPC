import zmq
from typing import List

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstancekey import TaskInstanceKey
import subprocess

from airflowHPC.utils.serialization import serialize_call, deserialize_call


class ZmqSequentialExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        self.context = zmq.Context()

        # Socket to send messages to
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:5557")

        # Socket to receive messages on
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect("tcp://localhost:5557")

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: List[str],
        queue=None,
        executor_config=None,
    ) -> None:
        self.validate_airflow_tasks_run_command(command)
        self.sender.send_string(serialize_call(key, command))

    def sync(self) -> None:
        while True:
            try:
                serialized = self.receiver.recv()
                key, command = deserialize_call(serialized)
            except EOFError:
                self.log.info(
                    "Failed to read tasks from the task queue because the other "
                    "end has closed the connection. Terminating worker %s.",
                    "MyWorker",
                )
                break
            try:
                if key is None or command is None:
                    # Received poison pill, no more tasks to run
                    break
                self.log.info("%s running %s", self.__class__.__name__, command)
                subprocess.check_call(command, close_fds=True)
                self.change_state(key, TaskInstanceState.SUCCESS)
            except subprocess.CalledProcessError as e:
                self.change_state(key, TaskInstanceState.FAILED)
                self.log.error("Failed to execute task %s.", e)
            finally:
                break

    def end(self):
        """End the executor."""
        self.sender.send_string(serialize_call(None, []))
        self.sender.close()
        self.receiver.close()
