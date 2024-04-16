from __future__ import annotations

import os
import sys
import shutil
import inspect
import subprocess

from pathlib import Path
from tempfile import TemporaryDirectory
from types import ClassMethodDescriptorType
from typing import Any, Callable

from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
)
from airflow.models import BaseOperator
from airflow.models.baseoperator import partial as airflow_partial
from airflow.models.mappedoperator import OperatorPartial
from airflow.operators.python import ExternalPythonOperator
from airflow.utils.file import get_unique_dag_module_name
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import write_python_script


def pool_slots_partial(*args, **kwargs):
    if not kwargs.get("mpi_ranks"):
        raise ValueError("mpi_ranks is required and cannot be mapped")
    else:
        kwargs.update({"pool_slots": int(kwargs["mpi_ranks"])})
    return airflow_partial(*args, **kwargs)


class PoolPartialDescriptor:
    """
    A descriptor that guards against ``.partial`` being called on Task objects.
    This is copied from airflow.models.baseoperator but overrides the pool_slots
    parameter to be calculated from mpi_ranks.

    This class reimplements _PartialDescriptor from airflow.models.baseoperator
    because class methods cannot be overridden via inheritance, only via. e.g.,
    `_PartialDescriptor.class_method = pool_slots_partial`, but this doesn't work
    if you need to set different class methods for different operators.
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


class RadicalExternalPythonOperator(ExternalPythonOperator):
    partial: Callable[..., OperatorPartial] = PoolPartialDescriptor()  # type: ignore

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
        self.mpi_ranks = mpi_ranks
        kwargs.update({"pool_slots": int(self.mpi_ranks)})
        super().__init__(**kwargs)
        if mpi_executable is None:
            for executable in ["mpirun", "mpiexec", "srun"]:
                if shutil.which(executable):
                    self.mpi_executable = executable
                    break
        else:
            self.mpi_executable = mpi_executable

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
