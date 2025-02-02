from airflow.decorators import task

__all__ = [
    "validate_convert_mdp",
    "update_write_mdp_json",
    "update_write_mdp_json_from_file",
    "update_write_mdp_json_as_mdp",
    "update_write_mdp_json_as_mdp_from_file",
    "write_mdp_json_as_mdp",
]


def mdp2json(mdp_file_path):
    import re

    mdp_data = {}
    with open(mdp_file_path, "r") as mdp_file:
        for line in mdp_file:
            line = line.strip()
            # Ignore lines starting with a semicolon or empty lines
            if line and not line.startswith(";") and not line.startswith("#"):
                key, value = line.split("=", 1)
                key = key.strip()
                # Convert - to _ in key
                key = key.replace("-", "_")
                value = value.strip()
                # Remove inline comments if present
                value = re.sub(r";.*$", "", value)
                # Remove leading and trailing whitespace
                value = value.strip()
                # Convert numeric values to their appropriate types
                if re.match(r"^-?\d+$", value):
                    value = int(value)
                elif re.match(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$", value):
                    value = float(value)
                # If the value is a boolean, convert to Python bool
                elif value.lower() in ["true", "false"]:
                    value = value.lower() == "true"
                # If the value is an array, convert to Python list
                elif re.match(r"^\[.*\]$", value):
                    if " " in value:
                        value = value.split()
                    else:
                        value = [value]
                # Handle groups which can be specified as a single value or a space separated list
                elif "grps" in key or "-groups" in key:
                    if " " in value:
                        value = value.split()
                    else:
                        value = [value]
                # If string make value lowercase
                if isinstance(value, str):
                    if key not in ["couple_moltype"]:
                        value = value.lower()
                mdp_data[key.lower()] = value
    return mdp_data


def json2mdp(mdp_json_data, output_file: str = None):
    import tempfile

    if not output_file:
        output_file = tempfile.mkstemp(suffix=".mdp")[1]
    with open(output_file, "w") as mdp_file:
        for key, value in mdp_json_data.items():
            if isinstance(value, list):
                value = " ".join(value)
            mdp_file.write(f"{key} = {value}\n")
    return output_file


def validate_json_mdp(mdp_data):
    import jsonschema, json, os
    from jsonschema.validators import extend

    file_dir = os.path.abspath(os.path.dirname(__file__))
    schema_file_path = os.path.join(file_dir, "mdp_schema.json")
    with open(schema_file_path, "r") as schema_file:
        schema = json.load(schema_file)

    def case_insensitive_enum(validator, enums, instance, schema):
        if isinstance(instance, str) and "enum" in schema:
            if instance.lower() not in [e.lower() for e in schema["enum"]]:
                yield jsonschema.ValidationError(
                    f"{instance} is not one of {schema['enum']}"
                )
        else:
            for error in jsonschema.Draft7Validator.VALIDATORS["enum"](
                validator, enums, instance, schema
            ):
                yield error

    CaseInsensitiveEnumValidator = extend(
        jsonschema.Draft7Validator, {"enum": case_insensitive_enum}
    )

    jsonschema.validate(mdp_data, schema, cls=CaseInsensitiveEnumValidator)


@task
def update_write_mdp_json(mdp_data, update_dict, output_file: str = None):
    import json, tempfile

    mdp_data.update(update_dict)
    validate_json_mdp(mdp_data)
    if not output_file:
        output_file = tempfile.mkstemp(suffix=".json")[1]
    with open(output_file, "w") as output_file:
        json.dump(obj=mdp_data, fp=output_file, indent=4)
    return output_file


def update_write_mdp_json_from_file(
    mdp_json_file_path, update_dict, output_file: str = None
):
    import json

    with open(mdp_json_file_path, "r") as mdp_file:
        mdp_data = json.load(mdp_file)
    return update_write_mdp_json(mdp_data, update_dict, output_file)


def update_write_mdp_json_as_mdp(mdp_data, update_dict, output_file: str = None):
    mdp_data.update(update_dict)
    validate_json_mdp(mdp_data)
    return json2mdp(mdp_data, output_file)


@task
def update_write_mdp_json_as_mdp_from_file(
    mdp_json_file_path, update_dict: dict | str = None, output_file: str = None
):
    import json
    import logging
    import ast

    if isinstance(update_dict, str):
        update_dict = ast.literal_eval(update_dict)

    # Enforce underscore format for keys
    for key in list(update_dict):
        if "-" in key:
            new_key = key.replace("-", "_")
            update_dict[new_key] = update_dict.pop(key)
    logging.info(f"update: {update_dict}")
    with open(mdp_json_file_path, "r") as mdp_file:
        mdp_data = json.load(mdp_file)
    for key in list(mdp_data):
        if "-" in key:
            new_key = key.replace("-", "_")
            mdp_data[new_key] = mdp_data.pop(key)
    return update_write_mdp_json_as_mdp(mdp_data, update_dict, output_file)


@task
def write_mdp_json_as_mdp(mdp_data, output_file: str = None):
    validate_json_mdp(mdp_data)
    return json2mdp(mdp_data, output_file)


@task
def validate_convert_mdp(mdp_file_path, output_file_path: str):
    import json, os

    mdp_data = mdp2json(mdp_file_path)
    validate_json_mdp(mdp_data)

    output_dir = os.path.dirname(output_file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file_path, "w") as mdp_json_fp:
        json.dump(obj=mdp_data, fp=mdp_json_fp, indent=4)
    return mdp_data
