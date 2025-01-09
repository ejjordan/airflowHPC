from airflow import Dataset
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from dataclasses import dataclass
from typing import Union

__all__ = (
    "get_file",
    "run_gmxapi",
    "run_gmxapi_dataclass",
    "update_gmx_input",
    "prepare_gmx_input",
    "branch_task",
    "list_from_xcom",
    "dataset_from_xcom_dicts",
    "dict_from_xcom_dicts",
    "json_from_dataset_path",
    "branch_task_template",
    "evaluate_template_truth",
    "run_if_needed",
    "run_if_false",
    "unpack_inputs",
)


@dataclass
class GmxInputHolder:
    args: list
    input_files: dict
    output_files: dict
    output_dir: str
    simulation_id: int


@dataclass
class GmxRunInfoHolder:
    inputs: GmxInputHolder
    outputs: dict


@task(multiple_outputs=False, trigger_rule="none_failed")
def get_file(
    input_dir, file_name, use_ref_data: bool = True, check_exists: bool = False
):
    import os
    from airflowHPC.data import data_dir as data

    if use_ref_data:
        data_dir = os.path.abspath(os.path.join(data, input_dir))
    else:
        data_dir = os.path.abspath(input_dir)
    file_to_get = os.path.join(data_dir, file_name)
    if check_exists:
        if os.path.exists(file_to_get):
            return True
        else:
            return False
    if not os.path.exists(file_to_get):
        raise FileNotFoundError(f"File {file_to_get} does not exist")
    return file_to_get


@task(trigger_rule="none_failed")
def verify_files(input_dir, filename, mdp_options, step_number: int | None = None):
    """Workaround for steps where multiple files are expected."""
    import logging, os

    if step_number is not None:
        step_str = f"iteration_{step_number}/"
    else:
        step_str = ""
    input_files = [
        f"{input_dir}/{step_str}sim_{i}/{filename}" for i in range(len(mdp_options))
    ]
    for file in input_files:
        logging.info(f"Checking if {file} exists: {os.path.exists(file)}")
        if not os.path.exists(file):
            return False
    return True


def _run_gmxapi(
    args: list, input_files: dict, output_files: dict, output_dir: str, stdin=None
):
    import os
    import gmxapi
    import logging

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_files_paths = {
        f"{k}": f"{os.path.join(out_path, v)}" for k, v in output_files.items()
    }
    cwd = os.getcwd()
    os.chdir(out_path)
    gmx = gmxapi.commandline_operation(
        gmxapi.commandline.cli_executable(),
        args,
        input_files,
        output_files_paths,
        stdin,
    )
    gmx.run()
    logging.info(gmx.output.stderr.result())
    logging.info(gmx.output.stdout.result())
    os.chdir(cwd)
    assert all(
        [os.path.exists(gmx.output.file[key].result()) for key in output_files.keys()]
    )
    return gmx


@task(multiple_outputs=True)
def run_gmxapi(
    args: list, input_files: dict, output_files: dict, output_dir: str, stdin=None
):
    gmx = _run_gmxapi(args, input_files, output_files, output_dir, stdin)
    return {f"{key}": f"{gmx.output.file[key].result()}" for key in output_files.keys()}


@task(multiple_outputs=True, max_active_tis_per_dagrun=1)
def run_gmxapi_dataclass(input_data: GmxInputHolder):
    """Ideally this could be an overload with multipledispatch but that does not play well with airflow"""
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
    return asdict(GmxRunInfoHolder(inputs=input_data, outputs=run_output))


@task
def update_gmx_input(
    gmx_output: GmxRunInfoHolder,
    args: list,
    input_files_keys: dict,
    output_files: dict,
):
    from dataclasses import asdict

    # Add values to the input_files dictionary if the keys are present in the gmx_output
    input_files = {
        key: gmx_output["outputs"][value]
        for key, value in input_files_keys.items()
        if value in gmx_output["outputs"].keys()
    }
    # Ensure that we got all the requested input files
    assert all([key in input_files.keys() for key in input_files_keys.keys()])
    # Create the updated GmxapiInputHolder
    updated_input_holder = GmxInputHolder(
        args=args,
        input_files=input_files,
        output_files=output_files,
        output_dir=gmx_output["inputs"]["output_dir"],
        simulation_id=gmx_output["inputs"]["simulation_id"],
    )
    return asdict(updated_input_holder)


@task
def prepare_gmx_input(
    args: list,
    input_files: dict,
    output_files: dict,
    output_path_parts: list,
    num_simulations: Union[int, str],
):
    """
    Prepare a list of GmxapiInputHolder objects for running multiple simulations.

    Parameters
    ----------
    args : list
        The arguments to pass to the gmxapi command.
    input_files : dict
        The input files to pass to the gmxapi command.
    output_files : dict
        The output files to pass to the gmxapi command.
    output_path_parts : list
        The parts of the output path to join together.
        Allows for flexible naming of output directories.
        The only constraint is that the simulation_id is appended to the end of the final element.
    num_simulations : int or str
    """
    import os
    from dataclasses import asdict
    from copy import deepcopy
    from collections.abc import Iterable

    inputHolderList = []
    output_dir = "/".join(output_path_parts)

    for i in range(num_simulations):
        inputs = deepcopy(input_files)
        for key, value in input_files.items():
            if isinstance(value, str) and os.path.exists(value):
                continue
            if isinstance(value, Iterable):
                inputs[key] = value[i]
        inputHolderList.append(
            asdict(
                GmxInputHolder(
                    args=args,
                    input_files=inputs,
                    output_files=output_files,
                    output_dir=f"{output_dir}{i}",
                    simulation_id=i,
                )
            )
        )

    return inputHolderList


@task
def prepare_gmx_input_named(
    args: list,
    input_files: dict,
    output_files: dict,
    output_path_parts: list,
    names: list,
):
    """
    Prepare a list of GmxapiInputHolder objects for running multiple simulations.

    Parameters
    ----------
    args : list
        The arguments to pass to the gmxapi command.
    input_files : dict
        The input files to pass to the gmxapi command.
    output_files : dict
        The output files to pass to the gmxapi command.
    output_path_parts : list
        The parts of the output path to join together.
        Allows for flexible naming of output directories.
    names : list
        The names to append to the output directory.
    """
    import os
    from dataclasses import asdict
    from copy import deepcopy
    from collections.abc import Iterable

    inputHolderList = []
    output_dir = "/".join(output_path_parts)

    for i, name in enumerate(names):
        inputs = deepcopy(input_files)
        for key, value in input_files.items():
            if isinstance(value, str) and os.path.exists(value):
                continue
            if isinstance(value, Iterable):
                inputs[key] = value[i]
        inputHolderList.append(
            asdict(
                GmxInputHolder(
                    args=args,
                    input_files=inputs,
                    output_files=output_files,
                    output_dir=f"{output_dir}/{name}",
                    simulation_id=i,
                )
            )
        )

    return inputHolderList


@task.branch
def branch_task(
    truth_value: bool | list[bool], task_if_true: str, task_if_false: str
) -> str:
    from collections.abc import Iterable

    # Handle list-like truth values
    if isinstance(truth_value, Iterable):
        truth_value = all(truth_value)
    if truth_value:
        return task_if_true
    else:
        return task_if_false


@task.branch
def branch_task_template(statement: str, task_if_true: str, task_if_false: str) -> str:
    """
    Handle branching based on a jinja templated statement.
    This is potentially dangerous as it can execute arbitrary python code,
    so we check that there are no python identifiers in the statement.
    This is not foolproof, but it should catch most cases.
    """
    if any([word.isidentifier() for word in statement.split()]):
        raise ValueError("Template statement potentially contains python code")
    if len(statement.split()) > 3:
        raise ValueError("Template statement should be a simple comparison")
    truth_value = eval(statement)

    if truth_value:
        return task_if_true
    else:
        return task_if_false


@task
def evaluate_template_truth(statement: str) -> str:
    import logging

    """
    Handle branching based on a jinja templated statement.
    This is potentially dangerous as it can execute arbitrary python code,
    so we check that there are no python identifiers in the statement.
    This is not foolproof, but it should catch most cases.
    """
    if any([word.isidentifier() for word in statement.split()]):
        raise ValueError("Template statement potentially contains python code")
    if len(statement.split()) > 3:
        raise ValueError("Template statement should be a simple comparison")
    logging.info(f"Evaluating statement: {statement}")
    truth_value = eval(statement)
    return truth_value


@task
def list_from_xcom(values):
    return list(values)


@task
def dataset_from_xcom_dicts(
    output_dir: str, output_fn: str, list_of_dicts, dataset_structure
):
    import os
    import json

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, output_fn)
    output_data = list()
    for data_dict in list_of_dicts:
        data = dict()
        for title, key_name in dataset_structure.items():
            if key_name in data_dict:
                data[title] = data_dict[key_name]
            elif "outputs" in data_dict and key_name in data_dict["outputs"]:
                data[title] = data_dict["outputs"][key_name]
            elif "inputs" in data_dict and key_name in data_dict["inputs"]:
                data[title] = data_dict["inputs"][key_name]
            elif (
                "inputs" in data_dict and key_name in data_dict["inputs"]["input_files"]
            ):
                data[title] = data_dict["inputs"]["input_files"][key_name]
            else:
                raise KeyError(f"Key {key_name} not found in {data_dict}")
        output_data.append(data)
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, separators=(",", ": "))
    dataset = Dataset(uri=output_file)
    return dataset


@task(trigger_rule="none_failed")
def dict_from_xcom_dicts(list_of_dicts, dict_structure):
    import os

    output_data = list()
    for data_dict in list_of_dicts:
        data = dict()
        for title, key_name in dict_structure.items():
            if key_name in data_dict:
                data[title] = data_dict[key_name]
            elif "outputs" in data_dict and key_name in data_dict["outputs"]:
                if not os.path.isabs(data_dict["outputs"][key_name]):
                    data[title] = os.path.join(
                        data_dict["output_dir"], data_dict["outputs"][key_name]
                    )
                else:
                    data[title] = data_dict["outputs"][key_name]
            elif "inputs" in data_dict and key_name in data_dict["inputs"]:
                data[title] = data_dict["inputs"][key_name]
            elif "input_files" in data_dict and key_name in data_dict["input_files"]:
                data[title] = data_dict["input_files"][key_name]
            elif "output_files" in data_dict and key_name in data_dict["output_files"]:
                if not os.path.isabs(data_dict["output_files"][key_name]):
                    data[title] = os.path.join(
                        data_dict["output_dir"], data_dict["output_files"][key_name]
                    )
                else:
                    data[title] = data_dict["output_files"][key_name]
            else:
                raise KeyError(f"Key {key_name} not found in {data_dict}")
        output_data.append(data)
    return output_data


@task(trigger_rule="none_failed")
def json_from_dataset_path(
    dataset_path: str, key: str = None, allow_missing: bool = False
):
    import json, os

    if not os.path.exists(dataset_path):
        if allow_missing:
            return {}
        else:
            raise AirflowException(f"Dataset path {dataset_path} does not exist")
    with open(dataset_path, "r") as f:
        data = json.load(f)
    if not key:
        return data
    else:
        if len(data) == 1:
            return data[str(key)]
        else:
            return [d[key] for d in data]


@task
def xcom_lookup(dag_id, task_id, key, **context):
    import logging

    logging.info(f"Looking up '{key}' from '{task_id}' in '{dag_id}'")
    task_instance = context["task_instance"]
    logging.info(
        f"xcom values: {task_instance.xcom_pull(dag_id=dag_id, task_ids=task_id, include_prior_dates=True)}"
    )
    if key:
        logging.info(
            f"xcom key value: {task_instance.xcom_pull(dag_id=dag_id, task_ids=task_id, key=key, include_prior_dates=True)}"
        )
    xcom = task_instance.xcom_pull(
        dag_id=dag_id, task_ids=task_id, key=key, include_prior_dates=True
    )
    return xcom


@task(max_active_tis_per_dagrun=1000)
def add_to_dataset(
    output_dir: str, output_fn: str, new_data: dict, new_data_keys: list[str]
):
    import os
    import json

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, output_fn)
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            data = json.load(f)
    else:
        data = {}

    # Navigate through the nested keys
    nested_data = data
    for key in new_data_keys[:-1]:
        if key not in nested_data:
            nested_data[key] = {}
        nested_data = nested_data[key]

    # Assign the new data to the final key
    nested_data[new_data_keys[-1]] = new_data

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2, separators=(",", ": "))
    dataset = Dataset(uri=output_file)
    return dataset


@task
def unpack_param(param_name: str, **context):
    return context["task"].render_template(param_name, context)


@task
def unpack_mdp_options(param_name: str = "{{ params.mdp_options | list}}", **context):
    """
    It is not possible to use templating for mapped operators (e.g. calls to op.expand()).
    Thus, this task handles dynamic sizing of mdp options.
    """
    import ast

    mdp_options = context["task"].render_template(param_name, context)
    mdp_options_parsed = [
        ast.literal_eval(opt) if type(opt) is str else opt for opt in mdp_options
    ]
    return mdp_options_parsed


@task
def unpack_inputs(param_string: str = "{{ params.inputs.gro.directory }}", **context):
    """
    It is not possible to use templating for mapped operators (e.g. calls to op.expand()).
    Thus, this task prepares input files for grompp.
    """
    mdp_opts_range = range(
        len(context["task"].render_template("{{ params.mdp_options | list}}", context))
    )
    input_dir = context["task"].render_template(param_string, context)
    return [f"{input_dir}/sim_{i}" for i in mdp_opts_range]


@task_group
def run_if_needed(dag_id: str, dag_params: dict, dag_display_name: str = None):
    if dag_display_name is None:
        dag_display_name = dag_id
    is_dag_done = get_file.override(task_id=f"is_{dag_display_name}_done")(
        input_dir=dag_params["output_dir"],
        file_name=dag_params["expected_output"],
        use_ref_data=False,
        check_exists=True,
    )
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_{dag_display_name}",
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=2,
        trigger_rule="none_failed",
        conf=dag_params,
    )
    dag_done = EmptyOperator(
        task_id=f"{dag_display_name}_done", trigger_rule="none_failed"
    )
    dag_done_branch = branch_task.override(task_id=f"{dag_display_name}_done_branch")(
        truth_value=is_dag_done,
        task_if_true=dag_done.task_id,
        task_if_false=trigger_dag.task_id,
    )
    is_dag_done >> dag_done_branch >> [trigger_dag, dag_done]


@task_group
def run_if_false(
    dag_id,
    dag_params,
    truth_value: bool,
    wait_for_completion: bool = True,
    dag_display_name: str = None,
):
    if dag_display_name is None:
        dag_display_name = dag_id
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_{dag_display_name}",
        trigger_dag_id=dag_id,
        wait_for_completion=wait_for_completion,
        poke_interval=2,
        trigger_rule="none_failed",
        conf=dag_params,
    )
    dag_done = EmptyOperator(
        task_id=f"{dag_display_name}_done", trigger_rule="none_failed"
    )
    dag_done_branch = branch_task.override(
        task_id=f"{dag_display_name}_done_branch", trigger_rule="none_failed"
    )(
        truth_value=truth_value,
        task_if_true=dag_done.task_id,
        task_if_false=trigger_dag.task_id,
    )
    dag_done_branch >> [trigger_dag, dag_done]
