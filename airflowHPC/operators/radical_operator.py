from __future__ import annotations

from typing import Any, Callable, Sequence, TYPE_CHECKING, Collection, Mapping

from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.decorators.base import (
    task_decorator_factory,
    DecoratedOperator,
    TaskDecorator,
)
from airflow.utils.operator_helpers import KeywordParameters

from airflow.utils.context import Context, context_merge

from airflowHPC.dags.tasks import GmxapiInputHolder, _run_gmxapi, GmxapiRunInfoHolder

"""
        queue: str = "radical",
        executor_config={
            "RadicalExecutor": {
                "use_mpi": False,
                "ranks": 1,
                "cores_per_rank": 1,
            }
        },
"""


class RadicalOperator(PythonOperator):
    template_fields: Sequence[str] = (
        "templates_dict",
        "op_args",
        "op_kwargs",
        "input_data",
    )

    def __init__(
        self,
        *,
        templates_dict: dict[str, Any] | None = None,
        templates_exts: Sequence[str] | None = None,
        show_return_value_in_logs: bool = True,
        input_data: GmxapiInputHolder,
        **kwargs,
    ) -> None:
        super().__init__(python_callable=self.gmxapi_multi, **kwargs)
        self.python_callable = self.gmxapi_multi
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts
        self.show_return_value_in_logs = show_return_value_in_logs
        self.input_data = input_data
        self.arguments = input_data["args"]
        self.input_files = input_data["input_files"]
        self.output_files = input_data["output_files"]
        self.output_dir = input_data["output_dir"]

    def gmxapi_multi(self, input_data: GmxapiInputHolder):
        from dataclasses import asdict

        gmx = _run_gmxapi(
            args=input_data["args"],
            input_files=input_data["input_files"],
            output_files=input_data["output_files"],
            output_dir=input_data["output_dir"],
        )
        run_output = {
            f"{key}": f"{gmx.output.file[key].result()}"
            for key in input_data["output_files"].keys()
        }
        return asdict(GmxapiRunInfoHolder(inputs=input_data, outputs=run_output))

    def ouput_files(self):
        return self.output_files

    def ouput_dir(self):
        return self.output_dir

    def execute(self, context: Context) -> Any:
        from dataclasses import asdict

        context_merge(
            context, asdict(self.input_data), templates_dict=self.templates_dict
        )
        return_value = self.gmxapi_multi(input_data=self.input_data)
        # import ipdb;ipdb.set_trace()
        if self.show_return_value_in_logs:
            self.log.info("Done. Returned value was: %s", return_value)
        else:
            self.log.info("Done. Returned value not shown")

        return return_value


"""
class _RadicalDecoratedOperator(DecoratedOperator, RadicalOperator):
    custom_operator_name: str = "@task.radical"

    def __init__(self, *, python_callable, op_args, op_kwargs, **kwargs) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def radical_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_RadicalDecoratedOperator,
        **kwargs,
    )
"""
