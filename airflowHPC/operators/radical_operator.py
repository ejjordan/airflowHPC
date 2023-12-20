from __future__ import annotations

from typing import Any, Callable

from airflow.operators.python import PythonOperator
from airflow.decorators.base import (
    task_decorator_factory,
    DecoratedOperator,
    TaskDecorator,
)

from airflowHPC.utils.serialization import serialize_operator_call


class RadicalOperator(PythonOperator):
    def __init__(self, *, python_callable, op_args, op_kwargs, **kwargs) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )
        self.radical_call = self.radical_callable()

    def radical_callable(self) -> Any:
        call = serialize_operator_call(
            self.python_callable, self.op_args, self.op_kwargs
        )
        self.log.info(f"Radical call: {call}")
        return call


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
