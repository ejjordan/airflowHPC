import dill
import functools
import json
from typing import Any, Callable, Dict, List, TypedDict


class AirflowCall(TypedDict):
    key: str
    command: List[str]


class OperatorCall(TypedDict):
    func: Callable
    op_args: List[Any]
    op_kwargs: Dict[str, Any]


to_bytes = functools.partial(dill.dumps, byref=True, recurse=False)


def from_hex(x: str):
    return dill.loads(bytes.fromhex(x))


def serialize_airflow_call(key, command) -> str:
    pack = AirflowCall(
        key=to_bytes(key).hex(),
        command=[to_bytes(arg).hex() for arg in command],
    )
    return json.dumps(pack, separators=(",", ":"))


def serialize_operator_call(func, op_args, op_kwargs) -> str:
    pack = OperatorCall(
        func=to_bytes(func).hex(),
        op_args=[to_bytes(arg).hex() for arg in op_args],
        op_kwargs={key: to_bytes(value).hex() for key, value in op_kwargs.items()},
    )
    return json.dumps(pack, separators=(",", ":"))


def deserialize_call(serialized_call):
    pack = json.loads(serialized_call)
    return dill.loads(bytes.fromhex(pack["key"])), [
        from_hex(arg) for arg in pack["command"]
    ]
