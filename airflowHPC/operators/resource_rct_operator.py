from __future__ import annotations

import os
import time
import uuid
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
        self._uuid = str(uuid.uuid4())
        self.log.info(f"=== ResourceRCTOperator: __init__ {kwargs}")
        # kwargs.update({"cwd": output_dir})
        super().__init__(**kwargs)
        if (
            self.executor_config
            and gmx_arguments[0] not in ["mdrun", "mdrun_mpi"]
            and self.executor_config["cpus_per_task"] > 1
        ):
            self.executor_config["cpus_per_task"] = 1
            self.warn = f"Overriding 'cpus_per_task' to 1 for {gmx_arguments[0]} as it is not supported."

        self.mpi_ranks = kwargs.get("executor_config", {}).get("mpi_ranks", 1)
        self.cpus_per_task = kwargs.get("executor_config", {}).get("cpus_per_task", 1)

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
        self.show_return_value_in_logs = show_return_value_in_logs

    def check_add_args(self, arg: str, value: str):
        for i, gmx_arg in enumerate(self.gmx_arguments):
            if arg == gmx_arg:
                if value != self.gmx_arguments[i + 1]:
                    msg = f"Changing argument '{arg} {self.gmx_arguments[i + 1]}' to '{arg} {value}'."
                    msg += f"The mdrun flag '{arg}' is managed by the operator and user input will be overridden."
                    self.log.warning(msg)
                    self.gmx_arguments[i + 1] = value
                return
        self.gmx_arguments.extend([arg, value])

    def execute(self, context: Context):

        server_addr = os.environ.get("RCT_SERVER_URL")
        assert server_addr is not None, "RCT_SERVER_URL is not set"

        self.log.info(f"======= SERVERURL: {server_addr}")
        self._rct_client = ru.zmq.Client(server_addr)

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }

        if isinstance(self.gmx_arguments, (str, bytes)):
            self.gmx_arguments = [self.gmx_arguments]
        if self.gmx_arguments[0] in ["mdrun", "mdrun_mpi"]:
            self.check_add_args("-ntomp", str(self.executor_config["cpus_per_task"]))

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
                "cores_per_rank": self.cpus_per_task,
              # 'input_staging': input_files_paths,
              # "pre_exec": ['. ~/scalems/.env.task'],
                "output_staging": sds,
              # "named_env": "bs0"
            }
        )

        self.log.info("====================== submit td %d" % os.getpid())
        uid = self._client.submit(td.as_dict())
        self.log.info("====================== submitted %s" % uid)

        # timeout to avoid zombie tasks?
        timeout = 60 * 60  # FIXME
        start = time.time()
        task = None
        while time.time() - start < timeout:
            state, exit_code = self._client.check(uid)
            self.log.info("=== check %s: %s" % (uid, state))
            if state in rp.FINAL:
                break
            time.sleep(1)

        self.log.info("=== task completed")
        if state in [rp.FAILED, rp.CANCELED]:
            raise AirflowException(f"Command failed with a state {state}.")

        # NOTE: skip_on_exit_code is not available on the BaseOperator.  Do we
        #       need it here?
        # if result.exit_code in self.skip_on_exit_code:
        #     raise AirflowSkipException(
        #         f"Bash command returned exit code {result.exit_code}. Skipping."
        #     )

        if exit_code != 0:
            raise AirflowException(
                f"Bash command returned a non-zero exit code {exit_code}."
            )

        if self.show_return_value_in_logs:
            self.log.info(f"Done. Returned value was: {output_files_paths}")

        return output_files_paths


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
