from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence, Union, Iterable

from airflow.exceptions import AirflowSkipException
from airflow.utils import timezone

from airflowHPC.operators.resource_bash_operator import ResourceBashOperator
import radical.utils as ru
import radical.pilot as rp

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RadicalGmxOperator(ResourceBashOperator):
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
                if int(self.gmx_arguments[i + 1]) != int(
                    kwargs["executor_config"]["cpus_per_task"]
                ):
                    msg = f"Argument {arg} is '{self.gmx_arguments[i + 1]}', "
                    msg += "but must be the same as executor_config['cpus_per_task']: "
                    msg += f"'{kwargs['executor_config']['cpus_per_task']}'"
                    raise ValueError(msg)

    def execute(self, context: Context):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }
        if self.gmx_executable is None:
            try:
                from gmxapi.commandline import cli_executable

                self.gmx_executable = cli_executable()
            except ImportError:
                raise ImportError(
                    "The gmx_executable argument must be set if the gmxapi package is not installed."
                )
        # bash_path = shutil.which("bash") or "bash"
        # env = self.get_env(context)

        # self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"gmx_executable: {self.gmx_executable}")
        self.log.info(f"gmx_arguments: {self.gmx_arguments}")
        self.log.info(f"input_files: {self.input_files}")
        self.log.info(f"output_files: {output_files_paths}")
        # self.log.info(f"gpu_ids: {self.gpu_ids}")
        # self.log.info(f"hostname: {self.hostname}")

        rp_endpoint = os.environ.get("RP_ENDPOINT")

        if not rp_endpoint:
            raise ValueError("RP_ENDPOINT environment variable not set.")

        rp_client = ru.zmq.Client(url=rp_endpoint)

        td = rp.TaskDescription()
        td.executable = self.gmx_executable
        td.ranks = self.mpi_ranks
        td.arguments = self.gmx_arguments
        # td.input_staging = self.input_files
        # td.output_staging = output_files_paths
        self.log.info(f"TaskDescription: {td.as_dict()}")
        self.log.info(f"RP_ENDPOINT: {rp_endpoint}")
        self.log.info(f"RP_CLIENT: {rp_client}")

        uid = rp_client.request(
            cmd="rp_execute",
            task_description=td.as_dict(),
            key=timezone.utcnow().isoformat(),
        )

        if not uid:
            raise AirflowSkipException("Command failed")

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
        host_flag = "-host"
        if mpi_executable is None:
            mpi_executable = "mpirun"
        elif mpi_executable == "mpirun":
            pass
        elif mpi_executable == "mpiexec":
            pass
        elif mpi_executable == "srun":
            host_flag = "--nodelist"
        else:
            raise ValueError(
                f"Unrecognized mpi_executable: {mpi_executable}. Must be one of ['mpirun', 'mpiexec', 'srun']"
            )
        if isinstance(gmx_arguments, (str, bytes)):
            gmx_arguments = [gmx_arguments]

        if self.gpu_ids:
            gmx_arguments.extend(["-gpu_id", ",".join(map(str, self.gpu_ids))])

        call = list()
        call.append(mpi_executable)
        call.extend([self.num_ranks_flag, str(mpi_ranks)])
        call.extend([host_flag, self.hostname])
        call.append(gmx_executable)
        call.extend(gmx_arguments)
        call.extend(self.flatten_dict(input_files))
        call.extend(self.flatten_dict(output_files))
        return " ".join(map(str, call))
