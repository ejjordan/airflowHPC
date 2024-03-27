from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from dataclasses import dataclass

__all__ = (
    "get_file",
    "run_gmxapi",
    "run_gmxapi_dataclass",
    "update_gmxapi_input",
    "prepare_gmxapi_input",
    "branch_task",
    "branch_task_template",
    "run_if_needed",
    "run_if_false",
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
    if not os.path.exists(file_to_get):
        raise FileNotFoundError(f"File {file_to_get} does not exist")
    return file_to_get


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


@task(multiple_outputs=True, queue="radical")
def run_gmxapi(
    args: list, input_files: dict, output_files: dict, output_dir: str, stdin=None
):
    gmx = _run_gmxapi(args, input_files, output_files, output_dir, stdin)
    return {f"{key}": f"{gmx.output.file[key].result()}" for key in output_files.keys()}


@task(multiple_outputs=True, max_active_tis_per_dagrun=1, queue="radical")
def run_gmxapi_dataclass(input_data: GmxapiInputHolder):
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
def list_from_xcom(values):
    return list(values)


@task
def unpack_ref_t(**context):
    """
    It is not possible to use templating for mapped operators (e.g. calls to op.expand()).
    Thus, this task handles dynamic sizing of the ref_t_list.
    """
    temps_list = context["task"].render_template(
        "{{ params.ref_t_list | list}}", context
    )
    return list([{"ref_t": ref_t} for ref_t in temps_list])


@task_group
def run_if_needed(dag_id, dag_params):
    is_dag_done = get_file.override(task_id=f"is_{dag_id}_done")(
        input_dir=dag_params["output_dir"],
        file_name=dag_params["expected_output"],
        use_ref_data=False,
        check_exists=True,
    )
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf=dag_params,
    )
    dag_done = EmptyOperator(task_id=f"{dag_id}_done", trigger_rule="none_failed")
    dag_done_branch = branch_task.override(task_id=f"{dag_id}_done_branch")(
        truth_value=is_dag_done,
        task_if_true=dag_done.task_id,
        task_if_false=trigger_dag.task_id,
    )
    is_dag_done >> dag_done_branch >> [trigger_dag, dag_done]


@task_group
def run_if_false(dag_id, dag_params, truth_value: bool):
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf=dag_params,
    )
    dag_done = EmptyOperator(task_id=f"{dag_id}_done", trigger_rule="none_failed")
    dag_done_branch = branch_task.override(task_id=f"{dag_id}_done_branch")(
        truth_value=truth_value,
        task_if_true=dag_done.task_id,
        task_if_false=trigger_dag.task_id,
    )
    dag_done_branch >> [trigger_dag, dag_done]
