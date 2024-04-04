from __future__ import annotations

import os
import shutil
import warnings
from functools import cached_property
from types import ClassMethodDescriptorType
from typing import TYPE_CHECKING, Container, Sequence, Union, Iterable, Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.baseoperator import partial as airflow_partial
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.models.mappedoperator import OperatorPartial

if TYPE_CHECKING:
    from airflow.utils.context import Context


import contextlib
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen, run
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.base import BaseHook
from gmxapi.commandline import cli_executable

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self, **kwargs) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__(**kwargs)

    def run_command(
        self,
        command: list[str],
        stdin: str | None = None,
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
    ) -> SubprocessResult:
        self.log.info("Tmp dir root location: %s", gettempdir())
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix="airflowtmp"))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = run(
                command,
                input=stdin,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
                text=True,
            )

            self.log.info("Output:")
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                self.log.info("%s", self.sub_process.stdout)

            self.log.info(
                "Command exited with return code %s", self.sub_process.returncode
            )
            return_code: int = self.sub_process.returncode

        return SubprocessResult(exit_code=return_code, output=self.sub_process.stdout)

    def send_sigterm(self):
        """Send SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)


def pool_slots_partial(*args, **kwargs):
    if not kwargs.get("cpus_per_task"):
        raise ValueError("cpus_per_task is required")
    if not kwargs.get("mpi_ranks"):
        raise ValueError("mpi_ranks is required")
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


class RadicalGmxapiBashOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "mpi_executable",
        "mpi_ranks",
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
        "mpi_ranks": "py",
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
        mpi_executable: str | None = None,
        gmx_executable: str | None = None,
        mpi_ranks: int,
        cpus_per_task: int = 1,
        # gpus: list[int] = None,
        gmx_arguments: list,
        input_files: dict,
        output_files: dict,
        output_dir: str,
        stdin=None,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> None:
        kwargs.update({"pool_slots": mpi_ranks * cpus_per_task})
        super().__init__(**kwargs)
        self.gmx_executable = gmx_executable
        self.mpi_executable = mpi_executable
        self.gmx_arguments = gmx_arguments
        self.mpi_ranks = mpi_ranks
        self.cpus_per_task = cpus_per_task
        self.input_files = input_files
        self.output_files = output_files
        self.output_dir = output_dir
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
        for i, arg in enumerate(self.gmx_arguments):
            if arg in ["-ntomp", "-ntmpi", "-nt"]:
                if self.gmx_arguments[i + 1] != str(self.cpus_per_task):
                    raise ValueError(
                        f"Argument {arg} must be the same as cpus_per_task: {self.cpus_per_task}"
                    )

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
        return env

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def execute(self, context: Context):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        out_dir_full_path = os.path.abspath(self.output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in self.output_files.items()
        }
        if self.gmx_executable is None:
            self.gmx_executable = cli_executable()
        if self.mpi_executable is None:
            self.mpi_executable = "mpirun"
        self.log.info(f"mpi_executable: {self.mpi_executable}")
        self.log.info(f"mpi_ranks: {self.mpi_ranks}")
        self.log.info(f"gmx_executable: {self.gmx_executable}")
        self.log.info(f"gmx_arguments: {self.gmx_arguments}")
        self.log.info(f"input_files: {self.input_files}")
        self.log.info(f"output_files: {output_files_paths}")
        self.bash_command = self.create_call(
            gmx_executable=self.gmx_executable,
            gmx_arguments=self.gmx_arguments,
            mpi_executable=self.mpi_executable,
            mpi_ranks=self.mpi_ranks,
            input_files=self.input_files,
            output_files=output_files_paths,
        )
        bash_path = shutil.which("bash") or "bash"
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")
        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
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

    def flatten_dict(self, mapping: dict):
        for key, value in mapping.items():
            yield str(key)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                yield from [str(element) for element in value]
            else:
                yield value

    def create_call(
        self,
        gmx_executable=None,
        gmx_arguments=(),
        mpi_executable=None,
        mpi_ranks: int = 1,
        input_files: Union[dict, Iterable[dict]] = None,
        output_files: Union[dict, Iterable[dict]] = None,
    ) -> str:
        if gmx_executable is None:
            gmx_executable = cli_executable()
        if mpi_executable is None:
            mpi_executable = "mpirun"
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
