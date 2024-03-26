from __future__ import annotations

import os
import shutil
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars

if TYPE_CHECKING:
    from airflow.utils.context import Context


import contextlib
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.base import BaseHook

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self, **kwargs) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__(**kwargs)

    def run_command(
        self,
        command: list[str],
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

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info("Output:")
            line = ""
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    line = raw_line.decode(
                        output_encoding, errors="backslashreplace"
                    ).rstrip()
                    self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info(
                "Command exited with return code %s", self.sub_process.returncode
            )
            return_code: int = self.sub_process.returncode

        return SubprocessResult(exit_code=return_code, output=line)

    def send_sigterm(self):
        """Send SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)


class LocalBashOperator(BaseOperator):
    template_fields: Sequence[str] = ("bash_command", "env")
    template_fields_renderers = {"bash_command": "bash", "env": "json"}
    ui_color = "#f0ede4"

    def __init__(
        self,
        *,
        bash_command: str,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_exit_code: int | None = None,
        skip_on_exit_code: int | Container[int] | None = 99,
        cwd: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
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
            if skip_on_exit_code is not None
            else []
        )
        self.cwd = cwd
        self.append_env = append_env

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

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

    def execute(self, context: Context):
        # import logging, traceback
        # logging.info(f"Executing {self.__class__.__name__}")
        # logging.info(f"traceback: {traceback.print_stack()}")
        bash_path = shutil.which("bash") or "bash"
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")
        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
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
