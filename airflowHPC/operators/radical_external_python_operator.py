from __future__ import annotations

import os
import sys
import inspect
import subprocess

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
)
from airflow.operators.python import ExternalPythonOperator
from airflow.utils.file import get_unique_dag_module_name
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import write_python_script


class RadicalExternalPythonOperator(ExternalPythonOperator):
    def __init__(
        self,
        *,
        python: str | None = None,
        mpi_executable: str | None = None,
        mpi_ranks: str,
        **kwargs,
    ):
        if python is None:
            python = os.path.join(sys.prefix, "bin", "python")
            kwargs.update({"python": python})
        super().__init__(**kwargs)
        if mpi_executable is None:
            self.mpi_executable = "mpirun"
        else:
            self.mpi_executable = mpi_executable
        self.mpi_ranks = mpi_ranks

    def execute(self, context):
        return super().execute(context)

    def _execute_python_callable_in_subprocess(self, python_path: Path):
        with TemporaryDirectory(prefix="venv-call") as tmp:
            tmp_dir = Path(tmp)
            op_kwargs: dict[str, Any] = dict(self.op_kwargs)
            if self.templates_dict:
                op_kwargs["templates_dict"] = self.templates_dict
            input_path = tmp_dir / "script.in"
            output_path = tmp_dir / "script.out"
            string_args_path = tmp_dir / "string_args.txt"
            script_path = tmp_dir / "script.py"
            termination_log_path = tmp_dir / "termination.log"

            self._write_args(input_path)
            self._write_string_args(string_args_path)

            jinja_context = {
                "op_args": self.op_args,
                "op_kwargs": op_kwargs,
                "expect_airflow": self.expect_airflow,
                "pickling_library": self.pickling_library.__name__,
                "python_callable": self.python_callable.__name__,
                "python_callable_source": self.get_python_source(),
            }

            if inspect.getfile(self.python_callable) == self.dag.fileloc:
                jinja_context["modified_dag_module_name"] = get_unique_dag_module_name(
                    self.dag.fileloc
                )

            write_python_script(
                jinja_context=jinja_context,
                filename=os.fspath(script_path),
                render_template_as_native_obj=self.dag.render_template_as_native_obj,
            )

            try:
                execute_in_subprocess(
                    cmd=[
                        self.mpi_executable,
                        "-np",
                        self.mpi_ranks,
                        os.fspath(python_path),
                        os.fspath(script_path),
                        os.fspath(input_path),
                        os.fspath(output_path),
                        os.fspath(string_args_path),
                        os.fspath(termination_log_path),
                    ]
                )
            except subprocess.CalledProcessError as e:
                if e.returncode in self.skip_on_exit_code:
                    raise AirflowSkipException(
                        f"Process exited with code {e.returncode}. Skipping."
                    )
                elif (
                    termination_log_path.exists()
                    and termination_log_path.stat().st_size > 0
                ):
                    error_msg = (
                        f"Process returned non-zero exit status {e.returncode}.\n"
                    )
                    with open(termination_log_path) as file:
                        error_msg += file.read()
                    raise AirflowException(error_msg) from None
                else:
                    raise

            if 0 in self.skip_on_exit_code:
                raise AirflowSkipException("Process exited with code 0. Skipping.")

            return self._read_result(output_path)
