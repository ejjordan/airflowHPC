from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING, Sequence, Union, Iterable

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
        for i, arg in enumerate(self.gmx_arguments):
            if arg in ["-ntomp", "-ntmpi", "-nt"]:
                if int(self.gmx_arguments[i + 1]) != int(
                    kwargs["executor_config"]["cpus_per_task"]
                ):
                    msg = f"Argument {arg} is '{self.gmx_arguments[i + 1]}', "
                    msg += "but must be the same as executor_config['cpus_per_task']: "
                    msg += f"'{kwargs['executor_config']['cpus_per_task']}'"
                    raise ValueError(msg)
        self.show_return_value_in_logs = show_return_value_in_logs

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

        if isinstance(gmx_arguments, (str, bytes)):
            gmx_arguments = [gmx_arguments]
        if self.gpu_ids:
            gmx_arguments.extend(["-gpu_id", ",".join(map(str, self.gpu_ids))])

        call = list()
        call.append(mpi_executable)
        call.extend([self.num_ranks_flag, str(mpi_ranks)])
        call.extend(["--cpu-set", self.core_ids])
        if self.hostname:
            if "srun" in mpi_executable:
                host_flag = "--nodelist"
            else:
                host_flag = "-host"
            call.extend([host_flag, f"{self.hostname}:{self.mpi_ranks}"])
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
