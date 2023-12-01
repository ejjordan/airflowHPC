import dill
import functools
import json
import typing


class CallPack(typing.TypedDict):
    """A call pack is a dictionary that contains all the information needed to execute a call."""

    key: str
    command: typing.List[str]
    decoder: str
    encoder: str


to_bytes = functools.partial(dill.dumps, byref=True, recurse=False)


def from_hex(x: str):
    return dill.loads(bytes.fromhex(x))


def serialize_call(key, command) -> str:
    pack = CallPack(
        key=to_bytes(key).hex(),
        command=[to_bytes(arg).hex() for arg in command],
        decoder="dill",
        encoder="dill",
    )
    return json.dumps(pack, separators=(",", ":"))


def deserialize_call(serialized_call):
    pack = json.loads(serialized_call)
    return dill.loads(bytes.fromhex(pack["key"])), [
        from_hex(arg) for arg in pack["command"]
    ]
