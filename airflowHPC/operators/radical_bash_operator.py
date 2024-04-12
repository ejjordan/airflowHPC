from __future__ import annotations

import os
import shutil
import warnings
from functools import cached_property
from types import ClassMethodDescriptorType
from typing import TYPE_CHECKING, Container, Sequence, Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.baseoperator import partial as airflow_partial
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.models.mappedoperator import OperatorPartial
from airflowHPC.hooks.subprocess import SubprocessHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


def pool_slots_partial(*args, **kwargs):
    if not kwargs.get("cpus_per_task"):
        raise ValueError("cpus_per_task is required and cannot be mapped")
    if not kwargs.get("mpi_ranks"):
        raise ValueError("mpi_ranks is required and cannot be mapped")
    kwargs.update({"pool_slots": kwargs["mpi_ranks"] * kwargs["cpus_per_task"]})
    return airflow_partial(*args, **kwargs)


class PoolPartialDescriptor:
    """
    A descriptor that guards against ``.partial`` being called on Task objects.
    This is copied from airflow.models.baseoperator but overrides the pool_slots
    parameter to be calculated from mpi_ranks and cpus_per_task.
    """

    class_method: ClassMethodDescriptorType = pool_slots_partial

    def __get__(
        self, obj: BaseOperator, cls: type[BaseOperator] | None = None
    ) -> Callable[..., OperatorPartial]:
        # Call this "partial" so it looks nicer in stack traces.
        def partial(**kwargs):
            raise TypeError(
                "partial can only be called on Operator classes, not Tasks themselves"
            )

        if obj is not None:
            return partial
        return self.class_method.__get__(cls, cls)


class RadicalBashOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "bash_command",
        "mpi_executable",
        "env",
        "stdin",
        "cwd",
    )
    template_fields_renderers = {
        "bash_command": "bash",
        "mpi_executable": "bash",
        "env": "json",
        "stdin": "py",
        "cwd": "py",
    }
    ui_color = "#f0ede4"

    partial: Callable[..., OperatorPartial] = PoolPartialDescriptor()  # type: ignore

    def __init__(
        self,
        *,
        bash_command: str | None = None,
        mpi_executable: str | None = None,
        mpi_ranks: str,
        cpus_per_task: str,
        # gpus: list[int] = None,
        stdin=None,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> None:
        self.mpi_ranks = int(mpi_ranks)
        self.cpus_per_task = int(cpus_per_task)
        kwargs.update({"pool_slots": self.mpi_ranks * self.cpus_per_task})
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.mpi_executable = mpi_executable

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
        return env

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def execute(self, context: Context):
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                os.makedirs(self.cwd)
        if self.mpi_executable is None:
            self.mpi_executable = "mpirun"
        if self.bash_command is None:
            raise ValueError("bash_command is a required argument")
        self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"cpus_per_task: {self.cpus_per_task}")
        self.log.info(f"cwd: {self.cwd}")
        bash_path = shutil.which("bash") or "bash"

        env = self.get_env(context)
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
        call.extend(["-np", str(mpi_ranks)])
        call.append(bash_command)
        return " ".join(map(str, call))
