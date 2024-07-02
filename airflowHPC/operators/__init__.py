from .resource_bash_operator import ResourceBashOperator
from .resource_gmx_operator import ResourceGmxOperator

resource_operators = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
]


def is_resource_operator(operator_name: str):
    return operator_name in resource_operators


__all__ = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
    "is_resource_operator",
]
