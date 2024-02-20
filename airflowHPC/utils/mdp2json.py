from airflow.decorators import task

__all__ = [
    "validate_convert_mdp",
    "update_write_mdp_json",
    "update_write_mdp_json_from_file",
    "update_write_mdp_json_as_mdp",
    "update_write_mdp_json_as_mdp_from_file",
]


def mdp2json(mdp_file_path):
    import re #, json

    mdp_data = {}
    with open(mdp_file_path, "r") as mdp_file:
        for line in mdp_file:
            line = line.strip()
            # Ignore lines starting with a semicolon or empty lines
            if line and not line.startswith(";") and not line.startswith("#"):
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove inline comments if present
                value = re.sub(r";.*$", "", value)
                # Remove leading and trailing whitespace
                value = value.strip()
                # Convert numeric values to their appropriate types
                if re.match(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$", value):
                    value = float(value)
                elif re.match(r"^-?\d+$", value):
                    value = int(value)
                # If the value is a boolean, convert to Python bool
                elif value.lower() in ["true", "false"]:
                    value = value.lower() == "true"
                # If the value is an array, convert to Python list
                elif re.match(r"^\[.*\]$", value):
                    #value = json.loads(value)
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

    file_dir = os.path.abspath(os.path.dirname(__file__))
    schema_file_path = os.path.join(file_dir, "mdp_schema.json")
    with open(schema_file_path, "r") as schema_file:
        schema = json.load(schema_file)
    jsonschema.validate(mdp_data, schema)


#@task
def update_write_mdp_json(mdp_data, update_dict, output_file: str = None):
    import json, tempfile

    mdp_data.update(update_dict)
    validate_json_mdp(mdp_data)
    if not output_file:
        output_file = tempfile.mkstemp(suffix=".json")[1]
    with open(output_file, "w") as output_file:
        json.dump(mdp_data, output_file, indent=4)
    return output_file


#@task
def update_write_mdp_json_from_file(
    mdp_json_file_path, update_dict, output_file: str = None
):
    import json

    with open(mdp_json_file_path, "r") as mdp_file:
        mdp_data = json.load(mdp_file)
    return update_write_mdp_json(mdp_data, update_dict, output_file)


#@task
def update_write_mdp_json_as_mdp(mdp_data, update_dict, output_file: str = None):
    mdp_data.update(update_dict)
    validate_json_mdp(mdp_data)
    return json2mdp(mdp_data, output_file)


@task
def update_write_mdp_json_as_mdp_from_file(
    mdp_json_file_path, update_dict, output_file: str = None
):
    import json

    with open(mdp_json_file_path, "r") as mdp_file:
        mdp_data = json.load(mdp_file)
    return update_write_mdp_json_as_mdp(mdp_data, update_dict, output_file)


#@task
def validate_convert_mdp(mdp_file_path):
    import json

    mdp_data = mdp2json(mdp_file_path)
    validate_json_mdp(mdp_data)
    with open("mdp_data.json", "w") as mdp_json:
        json.dump(mdp_data, mdp_json, indent=4)
    return mdp_data
