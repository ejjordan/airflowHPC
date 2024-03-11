from airflow.decorators import task
from dataclasses import dataclass

__all__ = (
    "get_file",
    "run_gmxapi",
    "run_gmxapi_dataclass",
    "update_gmxapi_input",
    "prepare_gmxapi_input",
    "branch_task",
)


@dataclass
class GmxapiInputHolder:
    args: list
    input_files: dict
    output_files: dict
    output_dir: str
    simulation_id: int


@dataclass
class GmxapiRunInfoHolder:
    inputs: GmxapiInputHolder
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
    assert os.path.exists(file_to_get)
    return file_to_get


def _run_gmxapi(args: list, input_files: dict, output_files: dict, output_dir: str):
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
        gmxapi.commandline.cli_executable(), args, input_files, output_files_paths
    )
    gmx.run()
    logging.info(gmx.output.stderr.result())
    logging.info(gmx.output.stdout.result())
    os.chdir(cwd)
    assert all(
        [os.path.exists(gmx.output.file[key].result()) for key in output_files.keys()]
    )
    return gmx


@task(multiple_outputs=True, queue="radical")
def run_gmxapi(args: list, input_files: dict, output_files: dict, output_dir: str):
    gmx = _run_gmxapi(args, input_files, output_files, output_dir)
    return {f"{key}": f"{gmx.output.file[key].result()}" for key in output_files.keys()}


@task(multiple_outputs=True, max_active_tis_per_dagrun=1, queue="radical")
def run_gmxapi_dataclass(input_data):
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
    return asdict(GmxapiRunInfoHolder(inputs=input_data, outputs=run_output))


@task
def update_gmxapi_input(
    gmxapi_output: GmxapiRunInfoHolder,
    args: list,
    input_files_keys: dict,
    output_files: dict,
):
    from dataclasses import asdict

    # Add values to the input_files dictionary if the keys are present in the gmxapi_output
    input_files = {
        key: gmxapi_output["outputs"][value]
        for key, value in input_files_keys.items()
        if value in gmxapi_output["outputs"].keys()
    }
    # Ensure that we got all the requested input files
    assert all([key in input_files.keys() for key in input_files_keys.keys()])
    # Create the updated GmxapiInputHolder
    updated_input_holder = GmxapiInputHolder(
        args=args,
        input_files=input_files,
        output_files=output_files,
        output_dir=gmxapi_output["inputs"]["output_dir"],
        simulation_id=gmxapi_output["inputs"]["simulation_id"],
    )
    return asdict(updated_input_holder)


@task
def prepare_gmxapi_input(
    args: list,
    input_files: dict,
    output_files: dict,
    output_dir: str,
    counter: int,
    num_simulations: int,
):
    import os
    from dataclasses import asdict
    from copy import deepcopy
    from collections.abc import Iterable

    inputHolderList = []

    for i in range(num_simulations):
        inputs = deepcopy(input_files)
        for key, value in input_files.items():
            if isinstance(value, str) and os.path.exists(value):
                continue
            if isinstance(value, Iterable):
                inputs[key] = value[i]
        inputHolderList.append(
            asdict(
                GmxapiInputHolder(
                    args=args,
                    input_files=inputs,
                    output_files=output_files,
                    output_dir=f"{output_dir}/step_{counter}/sim_{i}",
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


@task
def list_from_xcom(values):
    return list(values)
