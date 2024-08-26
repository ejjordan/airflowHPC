from __future__ import annotations

import os
import time
import threading as mt
from typing import TYPE_CHECKING, Sequence, Union, Iterable, Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.mappedoperator import OperatorPartial

from airflowHPC.dags.tasks import GmxapiInputHolder, GmxapiRunInfoHolder

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.models.baseoperator import BaseOperator

import radical.pilot as rp
import radical.utils as ru


class ResourceRCTOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "gmx_executable",
        "gmx_arguments",
        "input_files",
        "output_files",
        "output_dir",
    )
    template_fields_renderers = {
        "gmx_executable": "bash",
        "gmx_arguments": "py",
        "input_files": "py",
        "output_files": "py",
        "output_dir": "py",
    }
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        gmx_executable: str | None = None,
        gmx_arguments: list,
        input_files: dict,
        output_files: dict,
        output_dir: str,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:
        self.log.info(f"=== ResourceRCTOperator: __init__ {kwargs}")
      # kwargs.update({"cwd": output_dir})
        super().__init__(**kwargs)
        self.config = kwargs.get("executor_config", {})
        self.gmx_executable = gmx_executable
        self.gmx_arguments = gmx_arguments
        self.input_files = input_files
        self.output_files = output_files
        self.output_dir = output_dir
        self._rct_event = mt.Event()
        self._rct_task = None

    def execute(self, context: Context):

        pub_address = os.environ.get('RCT_PUB_URL')
        sub_address = os.environ.get('RCT_SUB_URL')

        assert pub_address is not None, "RCT_PUB_URL is not set"
        assert sub_address is not None, "RCT_SUB_URL is not set"

        self.log.info(f"======= SUB URL: {sub_address} {os.getpid()}")
        self._rct_sub = ru.zmq.Subscriber(channel='rct', url=sub_address)
        self._rct_sub.subscribe('update', cb=self._update_cb)

        self.log.info(f"======= PUB URL: {pub_address}")
        self._rct_pub = ru.zmq.Publisher(channel='rct', url=pub_address)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }
        self.log.info(f"mpi_ranks   : {self.config['mpi_ranks']}")
        self.log.info(f"gmx_executable  : {self.gmx_executable}")
        self.log.info(f"gmx_arguments   : {self.gmx_arguments}")
        self.log.info(f"input_files : {self.input_files}")
        self.log.info(f"output_files: {output_files_paths}")

        args = self.gmx_arguments
        for k,v in self.input_files.items():
            args += [k, v]

        for k,v in self.output_files.items():
            args += [k, v]

        td = rp.TaskDescription({
                'executable': self.gmx_executable,
                'arguments': args,
                'ranks': self.config['mpi_ranks'],
              # 'input_staging': self.input_files,
              # 'output_staging': output_files_paths,
        })

        self.log.info('====================== submit td %d' % os.getpid())
        self._rct_pub.put('request', {'td': td.as_dict()})

        # timeout to avoid zombie tasks?
        timeout = 60 * 60  # FIXME
        start = time.time()
        while time.time() - start < timeout:
            if self._rct_event.wait(timeout=1.0):
                self.log.info('=== waiting for task')
                break
            time.sleep(1)

        self.log.info('=== task done')
        if not self._rct_task:
            raise AirflowException("=== No result from RCT task.")

      # ec = self._rct_task['exit_code']
      # if ec in self.skip_on_exit_code:
      #     raise AirflowSkipException(f"Command exit code {ec} - skipping.")

        state = self._rct_task['state']
        if state in [rp.FAILED, rp.CANCELED]:
            raise AirflowException(f"Command failed with a state {state}.")

        return self._rct_task['stdout']


    def _update_cb(self, topic, msg):
        import pprint
        self.log.info("========================= update: %s" % pprint.pformat(msg))

        self._rct_task = msg['task']
        if msg['task']['state'] in rp.FINAL:
            self._rct_event.set()
            self.log.info(f"=== task done: {msg['task']['uid']}")
        else:
            self.log.info(f"=== task state: {msg['task']['state']}")



class ResourceRCTOperatorDataclass(ResourceRCTOperator):
    def __init__(self, *, input_data: GmxapiInputHolder, **kwargs) -> None:
        kwargs.update({"gmx_arguments": input_data["args"]})
        kwargs.update({"input_files": input_data["input_files"]})
        kwargs.update({"output_files": input_data["output_files"]})
        kwargs.update({"output_dir": input_data["output_dir"]})
        kwargs.update({"multiple_outputs": True})
        kwargs.update({"show_return_value_in_logs": False})
        super().__init__(
            **kwargs,
        )
        self.input_data = input_data

    def execute(self, context: Context):

        self.log.info("=== Dataclass operator executing")

        from dataclasses import asdict

        run_output = super().execute(context)
        output = asdict(GmxapiRunInfoHolder(inputs=self.input_data, outputs=run_output))
        self.log.info(f"Done. Returned value was: {output}")
        return output

