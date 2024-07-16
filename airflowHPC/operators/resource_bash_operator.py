from __future__ import annotations

import os
import shutil
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars

from airflowHPC.hooks.slurm import SlurmHook
from airflowHPC.hooks.subprocess import SubprocessHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ResourceBashOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "bash_command",
        "env",
        "stdin",
        "cwd",
    )
    template_fields_renderers = {
        "bash_command": "bash",
        "env": "json",
        "stdin": "py",
        "cwd": "py",
    }
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        bash_command: str | None = None,
        mpi_executable: str | None = None,
        stdin=None,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> None:
        if "executor_config" in kwargs:
            executor_config = kwargs.pop("executor_config")
            if "mpi_ranks" not in executor_config:
                raise ValueError(
                    "The executor_config argument must contain 'mpi_ranks'."
                )
            if "cpus_per_task" not in executor_config:
                executor_config["cpus_per_task"] = 1
            if (
                "gpu_type" in executor_config
                and executor_config["gpu_type"] is not None
            ):
                if executor_config["gpu_type"] not in ["rocm", "hip", "nvidia"]:
                    raise ValueError(
                        "The gpu_type argument must be set to 'rocm', 'hip', 'nvidia' or 'None' in executor_config."
                    )
                if "gpus" not in executor_config or executor_config["gpus"] == 0:
                    raise ValueError(
                        "The gpus argument must be set to a value greater than 0 in executor_config if gpu_type is set."
                    )
            if "gpus" in executor_config and executor_config["gpus"] > 0:
                if (
                    "gpu_type" not in executor_config
                    or executor_config["gpu_type"] is None
                ):
                    raise ValueError(
                        "The gpu_type argument must be set to 'rocm', 'hip', 'nvidia' in executor_config if gpus is set."
                    )
            kwargs.update({"executor_config": executor_config})
        else:
            raise ValueError(
                "The executor_config argument must contain 'mpi_ranks'. It may also contain 'cpus_per_task', 'gpus', and 'gpu_type'."
            )

        self.mpi_ranks = int(executor_config["mpi_ranks"])
        self.cpus_per_task = (
            int(executor_config["cpus_per_task"])
            if "cpus_per_task" in executor_config
            else 1
        )
        self.gpus = int(executor_config["gpus"]) if "gpus" in executor_config else 0
        self.gpu_type = (
            executor_config["gpu_type"] if "gpu_type" in executor_config else None
        )
        self.slurm_hook = SlurmHook()
        super().__init__(**kwargs)
        self.bash_command = bash_command
        if mpi_executable is None:
            for executable, num_ranks_flag in [
                ("mpirun", "-np"),
                ("mpiexec", "-np"),
                ("srun", "-n"),
            ]:
                if shutil.which(executable):
                    self.mpi_executable = executable
                    self.num_ranks_flag = num_ranks_flag
                    break
            if not hasattr(self, "mpi_executable"):
                msg = "Could not find mpirun, mpiexec, or srun in PATH. "
                msg += "Please check that one is loaded or specify a path to mpi_executable."
                raise ValueError(msg)
        else:
            self.mpi_executable = mpi_executable
            if "srun" in mpi_executable:
                self.num_ranks_flag = "--ntasks"
            else:
                self.num_ranks_flag = "-np"

        self.stdin = stdin
        self.env = env
        self.output_encoding = output_encoding
        if skip_exit_code is not None:
            warnings.warn(
                "skip_exit_code is deprecated. Please use skip_on_exit_code",
                DeprecationWarning,
                stacklevel=2,
            )
            skip_on_exit_code = skip_exit_code
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code
            else []
        )
        self.cwd = cwd
        self.append_env = append_env
        # This is set by the executor because it knows the available GPUs
        self.gpu_ids = []
        # This is also set by the executor
        self.hostname = ""

    def get_env(self, context):
        """Build the set of environment variables to be exposed for the bash command."""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        else:
            if self.append_env:
                system_env.update(env)
                env = system_env

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting env vars: %s",
            " ".join(f"{k}={v!r}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        env.update({"OMP_NUM_THREADS": str(self.cpus_per_task)})
        if self.gpu_type == None:
            if self.gpus > 0:
                raise ValueError("Set gpus > 0 but did not specify gpu_type.")
        if self.gpus > 0:
            if self.slurm_hook.gpu_env_var_name in env.keys():
                self.log.debug(f"visible: {env[self.slurm_hook.gpu_env_var_name]}")
                self.gpu_ids = [
                    int(id) for id in env[self.slurm_hook.gpu_env_var_name].split(",")
                ]
            else:
                raise RuntimeError(
                    f"Set gpu_type to {self.gpu_type} but did not specify gpu_ids. \
                    If you requested gpus and get this error, please contact the project maintainers."
                )
        if self.gpu_type == "rocm":
            env.update({"ROCR_VISIBLE_DEVICES": ",".join(map(str, self.gpu_ids))})
        if self.gpu_type == "hip":
            env.update({"HIP_VISIBLE_DEVICES": ",".join(map(str, self.gpu_ids))})
        if self.gpu_type == "nvidia":
            env.update({"CUDA_VISIBLE_DEVICES": ",".join(map(str, self.gpu_ids))})
        self.hostname = env.get(self.slurm_hook.hostname_env_var_name, "")
        return env

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def execute(self, context: Context):
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                os.makedirs(self.cwd)
        if self.bash_command is None:
            raise ValueError("bash_command is a required argument")
        bash_path = shutil.which("bash") or "bash"
        env = self.get_env(context)

        self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"cpus_per_task: {self.cpus_per_task}")
        self.log.info(f"cwd: {self.cwd}")
        self.log.info(f"gpu_ids: {self.gpu_ids}")

        self.call = self.create_call(
            mpi_executable=self.mpi_executable,
            mpi_ranks=self.mpi_ranks,
            bash_command=self.bash_command,
        )
        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.call],
            stdin=self.stdin,
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
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

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()

    def create_call(
        self,
        mpi_executable: str | None,
        mpi_ranks: int,
        bash_command: str,
    ) -> str:
        if mpi_executable is None:
            mpi_executable = "mpirun"
        try:
            mpi_executable = str(mpi_executable)
        except Exception as e:
            raise TypeError(
                "This operator requires paths and names to be strings. *executable* argument is "
                f"{type(mpi_executable)}."
            )

        call = list()
        call.append(mpi_executable)
        call.extend([self.num_ranks_flag, str(mpi_ranks)])
        call.append(bash_command)
        return " ".join(map(str, call))
