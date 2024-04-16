from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING, Sequence, Union, Iterable, Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.mappedoperator import OperatorPartial

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflowHPC.operators.radical_bash_operator import (
    RadicalBashOperator,
    PoolPartialDescriptor,
)


class RadicalGmxapiBashOperator(RadicalBashOperator):
    template_fields: Sequence[str] = (
        "mpi_executable",
        "gmx_executable",
        "gmx_arguments",
        "input_files",
        "output_files",
        "output_dir",
        "stdin",
        "env",
    )
    template_fields_renderers = {
        "gmx_executable": "bash",
        "mpi_executable": "bash",
        "gmx_arguments": "py",
        "input_files": "py",
        "output_files": "py",
        "output_dir": "py",
        "env": "json",
    }
    ui_color = "#f0ede4"

    partial: Callable[..., OperatorPartial] = PoolPartialDescriptor()  # type: ignore

    def __init__(
        self,
        *,
        gmx_executable: str | None = None,
        gmx_arguments: list,
        input_files: dict,
        output_files: dict,
        output_dir: str,
        **kwargs,
    ) -> None:
        kwargs.update({"cwd": output_dir})
        super().__init__(**kwargs)
        self.gmx_executable = gmx_executable
        self.gmx_arguments = gmx_arguments
        self.input_files = input_files
        self.output_files = output_files
        self.output_dir = output_dir
        for i, arg in enumerate(self.gmx_arguments):
            if arg in ["-ntomp", "-ntmpi", "-nt"]:
                if self.gmx_arguments[i + 1] != str(self.cpus_per_task):
                    raise ValueError(
                        f"Argument {arg} must be the same as cpus_per_task: {self.cpus_per_task}"
                    )

    def execute(self, context: Context):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }
        if self.gmx_executable is None:
            from gmxapi.commandline import cli_executable

            self.gmx_executable = cli_executable()
        self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"gmx_executable: {self.gmx_executable}")
        self.log.info(f"gmx_arguments: {self.gmx_arguments}")
        self.log.info(f"input_files: {self.input_files}")
        self.log.info(f"output_files: {output_files_paths}")
        self.bash_command = self.create_gmxapi_call(
            gmx_executable=self.gmx_executable,
            gmx_arguments=self.gmx_arguments,
            mpi_executable=self.mpi_executable,
            mpi_ranks=self.mpi_ranks,
            input_files=self.input_files,
            output_files=output_files_paths,
        )
        bash_path = shutil.which("bash") or "bash"
        env = self.get_env(context)
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
        return result.output

    def flatten_dict(self, mapping: dict):
        for key, value in mapping.items():
            yield str(key)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                yield from [str(element) for element in value]
            else:
                yield value

    def create_gmxapi_call(
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
        try:
            gmx_executable = str(gmx_executable)
        except Exception as e:
            raise TypeError(
                "This operator requires paths and names to be strings. *executable* argument is "
                f"{type(gmx_executable)}."
            )
        try:
            mpi_executable = str(mpi_executable)
        except Exception as e:
            raise TypeError(
                "This operator requires paths and names to be strings. *executable* argument is "
                f"{type(mpi_executable)}."
            )
        if isinstance(gmx_arguments, (str, bytes)):
            gmx_arguments = [gmx_arguments]

        call = list()
        call.append(mpi_executable)
        call.extend(["-np", str(mpi_ranks)])
        call.append(gmx_executable)
        call.extend(gmx_arguments)
        call.extend(self.flatten_dict(input_files))
        call.extend(self.flatten_dict(output_files))
        return " ".join(map(str, call))
