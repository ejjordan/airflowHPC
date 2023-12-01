from __future__ import annotations

from typing import Any, Callable, Collection, Mapping, Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import KeywordParameters
from airflow.decorators.base import task_decorator_factory, DecoratedOperator

from scalems.call import serialize_call


class RadicalPythonOperator(BaseOperator):
    """
    Executes a Python callable with radical.pilot backend.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`

    When running your callable, Airflow will pass a set of keyword arguments that can be used in your
    function. This set of kwargs correspond exactly to what you can use in your jinja templates.
    For this to work, you need to define ``**kwargs`` in your function header, or you can add directly the
    keyword arguments you would like to get - for example with the below code your callable will get
    the values of ``ti`` and ``next_ds`` context variables.

    With explicit arguments:

    .. code-block:: python

       def my_python_callable(ti, next_ds):
           pass

    With kwargs:

    .. code-block:: python

       def my_python_callable(**kwargs):
           ti = kwargs["ti"]
           next_ds = kwargs["next_ds"]


    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param show_return_value_in_logs: a bool value whether to show return_value
        logs. Defaults to True, which allows return value log output.
        It can be set to False to prevent log output of return value when you return huge data
        such as transmission a large amount of XCom to TaskAPI.
    """

    template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")
    template_fields_renderers = {
        "templates_dict": "json",
        "op_args": "py",
        "op_kwargs": "py",
    }
    BLUE = "#ffefeb"
    ui_color = BLUE

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs: Sequence[str] = (
        "python_callable",
        "op_kwargs",
    )

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        templates_dict: dict[str, Any] | None = None,
        templates_exts: Sequence[str] | None = None,
        show_return_value_in_logs: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not callable(python_callable):
            raise AirflowException("`python_callable` param must be callable")
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts
        self.show_return_value_in_logs = show_return_value_in_logs

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs, templates_dict=self.templates_dict)
        self.op_kwargs = self.determine_kwargs(context)

        return_value = self.execute_callable()
        # return_value = self.radical_callable()
        if self.show_return_value_in_logs:
            self.log.info("Done. Returned value was: %s", return_value)
        else:
            self.log.info("Done. Returned value not shown")

        return return_value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(
            self.python_callable, self.op_args, context
        ).unpacking()

    def execute_callable(self) -> Any:
        """
        Call the python callable with the given arguments.

        :return: the return value of the call.
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)

    def radical_callable(self) -> Any:
        call = serialize_call(
            func=self.python_callable,
            args=self.op_args,
            kwargs=self.op_kwargs,
            requirements=None,
        )
        return call


class _RadicalPythonDecoratedOperator(DecoratedOperator, RadicalPythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """

    template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")
    template_fields_renderers = {
        "templates_dict": "json",
        "op_args": "py",
        "op_kwargs": "py",
    }

    custom_operator_name: str = "@task"

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
) -> "TaskDecorator":
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_RadicalPythonDecoratedOperator,
        **kwargs,
    )
