from __future__ import annotations

import os
import time
import pprint
import threading as mt
from typing import TYPE_CHECKING, Sequence, Iterable

from airflow.exceptions import AirflowException

from airflowHPC.dags.tasks import GmxInputHolder, GmxRunInfoHolder

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

        self.mpi_ranks = kwargs.get("executor_config", {}).get("mpi_ranks", 1)

        if gmx_executable is None:
            try:
                from gmxapi.commandline import cli_executable

                gmx_executable = cli_executable()
            except ImportError:
                raise ImportError(
                    "The gmx_executable argument must be set if the gmxapi package is not installed."
                )

        self.gmx_executable = gmx_executable
        self.gmx_arguments = gmx_arguments
        self.input_files = input_files
        self.output_files = output_files
        self.output_dir = output_dir
        self._rct_event = mt.Event()
        self._rct_task = None
        self.show_return_value_in_logs = show_return_value_in_logs

    def execute(self, context: Context):
        pub_address = os.environ.get("RCT_PUB_URL")
        sub_address = os.environ.get("RCT_SUB_URL")

        assert pub_address is not None, "RCT_PUB_URL is not set"
        assert sub_address is not None, "RCT_SUB_URL is not set"

        self.log.info(f"======= SUB URL: {sub_address} {os.getpid()}")
        self._rct_sub = ru.zmq.Subscriber(channel="rct", url=sub_address)
        self._rct_sub.subscribe("update", cb=self._update_cb)

        self.log.info(f"======= PUB URL: {pub_address}")
        self._rct_pub = ru.zmq.Publisher(channel="rct", url=pub_address)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }
        self.log.info(f"mpi_ranks         : {self.mpi_ranks}")
        self.log.info(f"gmx_executable    : {self.gmx_executable}")
        self.log.info(f"gmx_arguments     : {self.gmx_arguments}")
        self.log.info(f"input_files       : {self.input_files}")
        self.log.info(f"output_files      : {self.output_files}")
        self.log.info(f"output_dir        : {self.output_dir}")
        self.log.info(f"output_files_paths: {output_files_paths}")

        args = self.gmx_arguments
        args.extend(self.flatten_dict(self.input_files))
        args.extend(self.flatten_dict(self.output_files))

        sds = list()
        for f in output_files_paths.values():
            sds.append(
                {
                    "source": os.path.basename(f),
                    "target": out_dir_full_path,
                    "action": rp.TRANSFER,
                }
            )

        td = rp.TaskDescription(
            {
                "executable": self.gmx_executable,
                "arguments": args,
                "ranks": self.mpi_ranks,
                # 'input_staging': input_files_paths,
                "output_staging": sds,
            }
        )

        self.log.info("====================== submit td %d" % os.getpid())
        self._rct_pub.put("request", {"td": td.as_dict()})

        # timeout to avoid zombie tasks?
        timeout = 60 * 60  # FIXME
        start = time.time()
        while time.time() - start < timeout:
            if self._rct_event.wait(timeout=1.0):
                self.log.info("=== waiting for task")
                break
            time.sleep(1)

        self.log.info("=== task completed")
        if not self._rct_task:
            raise AirflowException("=== No result from RCT task.")

        state = self._rct_task["state"]
        if state in [rp.FAILED, rp.CANCELED]:
            raise AirflowException(f"Command failed with a state {state}.")

        # NOTE: skip_on_exit_code is not available on the BaseOperator.  Do we
        #       need it here?
        # if result.exit_code in self.skip_on_exit_code:
        #     raise AirflowSkipException(
        #         f"Bash command returned exit code {result.exit_code}. Skipping."
        #     )

        exit_code = self._rct_task["exit_code"]
        if exit_code != 0:
            raise AirflowException(
                f"Bash command returned a non-zero exit code {exit_code}."
            )

        if self.show_return_value_in_logs:
            self.log.info(f"Done. Returned value was: {output_files_paths}")

        return output_files_paths

    def _update_cb(self, topic, msg):
        task = msg["task"]
        uid = task["uid"]
        state = task["state"]
        self.log.info("===================== update %s: %s" % (uid, state))

        self._rct_task = msg["task"]

        if state in rp.FINAL:
            self.log.info("=== task %s: \n%s" % (uid, pprint.pformat(task)))
            self._rct_event.set()

    def flatten_dict(self, mapping: dict):
        for key, value in mapping.items():
            yield str(key)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                yield from [str(element) for element in value]
            else:
                yield value


class ResourceRCTOperatorDataclass(ResourceRCTOperator):
    def __init__(self, *, input_data: GmxInputHolder, **kwargs) -> None:
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
        output = asdict(GmxRunInfoHolder(inputs=self.input_data, outputs=run_output))
        self.log.info(f"Done. Returned value was: {output}")
        return output
