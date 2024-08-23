from .resource_bash_operator import ResourceBashOperator
from .resource_gmx_operator import ResourceGmxOperator
from .resource_rct_operator import ResourceRCTOperator

resource_operators = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
    "ResourceRCTOperator",
]


def is_resource_operator(operator_name: str):
    return operator_name in resource_operators


__all__ = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
    "ResourceRCTOperator",
    "is_resource_operator",
]
