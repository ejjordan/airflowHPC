from __future__ import annotations

import os
import shutil
from functools import cached_property
from typing import TYPE_CHECKING, Sequence, Union, Iterable

from airflow.configuration import conf
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.exceptions import AirflowException, AirflowSkipException

from airflowHPC.dags.tasks import GmxInputHolder, GmxRunInfoHolder
from airflowHPC.operators import ResourceBashOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ResourceGmxOperator(ResourceBashOperator):
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
        kwargs.update({"cwd": output_dir})
        super().__init__(**kwargs)
        if (
            self.executor_config
            and gmx_arguments[0] not in ["mdrun", "mdrun_mpi"]
            and self.executor_config["cpus_per_task"] > 1
        ):
            self.executor_config["cpus_per_task"] = 1
            self.warn = f"Overriding 'cpus_per_task' to 1 for {gmx_arguments[0]} as it is not supported."
        if gmx_executable is None:
            try:
                from gmxapi.commandline import cli_executable

                self.gmx_executable = cli_executable()
            except ImportError:
                raise ImportError(
                    "The gmx_executable argument must be set if the gmxapi python package is not installed."
                )
        elif gmx := self._exec_check(gmx_executable):
            self.gmx_executable = gmx
        else:
            raise ValueError(f"Executable {gmx_executable} not found.")

        self.gmx_arguments = gmx_arguments
        self.input_files = input_files
        self.output_files = output_files
        self.output_dir = output_dir
        for arg in self.gmx_arguments:
            if arg in ["-ntmpi", "-nt"]:
                msg = f"{self.__class__.__name__} is designed for MPI versions of GROMACS and does not support {arg}\n"
                msg += f"The number of OpenMP threads (flag '-ntomp') is managed by this operator and need not be set."
                raise ValueError(msg)

        self.show_return_value_in_logs = show_return_value_in_logs

    @cached_property
    @providers_configuration_loaded
    def allow_dispersed_cores(self) -> bool:
        return conf.getboolean("hpc", "allow_dispersed_cores", fallback=True)

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
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }

        bash_path = shutil.which("bash") or "bash"
        env = self.get_env(context)

        if isinstance(self.gmx_arguments, (str, bytes)):
            self.gmx_arguments = [self.gmx_arguments]
        if self.gmx_arguments[0] in ["mdrun", "mdrun_mpi"]:
            self.check_add_args("-ntomp", str(self.executor_config["cpus_per_task"]))
            ranks_id = self.core_ids.split(",")
            if not self.allow_dispersed_cores:
                self.check_add_args("-pin", "on")
                self.check_add_args("-pinoffset", ranks_id[0])
            elif self.allow_dispersed_cores and all(
                x <= y for x, y in zip(ranks_id, ranks_id[1:])
            ):
                self.check_add_args("-pin", "on")
                self.check_add_args("-pinoffset", ranks_id[0])
            else:
                self.log.error(
                    f"mdrun pinning only works with sequential rank ids, try setting allow_dispersed_cores to False"
                )
                raise AirflowException(f"Could not pin cores for mdrun")
        if "warn" in self.__dict__ and self.warn:
            self.log.warning(self.warn)
        if self.gpu_ids:
            self.gmx_arguments.extend(["-gpu_id", ",".join(map(str, self.gpu_ids))])

        self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"gmx_executable: {self.gmx_executable}")
        self.log.info(f"gmx_arguments: {self.gmx_arguments}")
        self.log.info(f"input_files: {self.input_files}")
        self.log.info(f"output_files: {output_files_paths}")
        self.log.info(f"core_ids: {self.core_ids}")
        self.log.info(f"gpu_ids: {self.gpu_ids}")
        self.log.info(f"hostname: {self.hostname}")

        assert shutil.which(self.gmx_executable) is not None
        assert shutil.which(self.mpi_executable) is not None
        self.bash_command = self.create_gmx_call(
            gmx_executable=self.gmx_executable,
            gmx_arguments=self.gmx_arguments,
            mpi_executable=self.mpi_executable,
            mpi_ranks=self.mpi_ranks,
            input_files=self.input_files,
            output_files=output_files_paths,
        )
        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
            stdin=self.stdin,
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.output_dir,
        )

        if result.exit_code in self.skip_on_exit_code:
            raise AirflowSkipException(
                f"Bash command returned exit code {result.exit_code}. Skipping."
            )
        elif result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
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

    def create_gmx_call(
        self,
        gmx_executable: str,
        gmx_arguments: list | str | bytes,
        mpi_executable: str,
        mpi_ranks: int,
        input_files: Union[dict, Iterable[dict]] = None,
        output_files: Union[dict, Iterable[dict]] = None,
    ) -> str:
        if input_files is None:
            input_files = {}
        if output_files is None:
            output_files = {}

        call = list()
        call.append(mpi_executable)
        call.extend([self.num_ranks_flag, str(mpi_ranks)])
        if self.hostname:
            if "srun" in mpi_executable:
                host_flag = "--nodelist"
                call.extend([host_flag, f"{self.hostname}"])
            else:
                host_flag = "-host"
                call.extend([host_flag, f"{self.hostname}:{self.mpi_ranks}"])
                call.extend(["--cpu-set", self.core_ids])
        call.append(gmx_executable)
        call.extend(gmx_arguments)
        call.extend(self.flatten_dict(input_files))
        call.extend(self.flatten_dict(output_files))
        return " ".join(map(str, call))


class ResourceGmxOperatorDataclass(ResourceGmxOperator):
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
        from dataclasses import asdict

        run_output = super().execute(context)
        output = asdict(GmxRunInfoHolder(inputs=self.input_data, outputs=run_output))
        self.log.info(f"Done. Returned value was: {output}")
        return output
