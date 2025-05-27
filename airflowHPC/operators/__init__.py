from .resource_bash_operator import ResourceBashOperator
from .resource_gmx_operator import ResourceGmxOperator, ResourceGmxOperatorDataclass
from .resource_rct_operator import ResourceRCTOperator, ResourceRCTOperatorDataclass

resource_operators = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
    "ResourceGmxOperatorDataclass",
]

resource_rct_operators = [
    "ResourceRCTOperator",
    "ResourceRCTOperatorDataclass",
]


def is_resource_operator(operator_name: str):
    return operator_name in resource_operators


def is_resource_rct_operator(operator_name: str):
    return operator_name in resource_rct_operators


__all__ = [
    "ResourceBashOperator",
    "ResourceGmxOperator",
    "ResourceGmxOperatorDataclass",
    "ResourceRCTOperator",
    "ResourceRCTOperatorDataclass",
    "is_resource_operator",
    "is_resource_rct_operator",
]
