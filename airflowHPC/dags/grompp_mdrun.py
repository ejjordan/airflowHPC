from airflow import DAG
from airflow.models.param import Param
from airflow.utils.timezone import datetime
from airflowHPC.dags.tasks import (
    prepare_gmx_input,
    update_gmx_input,
    xcom_lookup,
    dataset_from_xcom_dicts,
)
from airflowHPC.operators import ResourceGmxOperatorDataclass


with DAG(
    "grompp_mdrun",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {
                    "task_id": "get_mdp",
                    "key": None,
                },
                "gro": {
                    "task_id": "get_gro",
                    "key": None,
                },
                "top": {
                    "task_id": "get_top",
                    "key": None,
                },
                "parent_dag_id": "dag",
            },
            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
                "properties": {"mdp": {"type": ["object", "null"]}},
                "required": ["mdp"],
            },
            section="inputs",
        ),
        "num_simulations": 4,
        "output_dir": "outputs",
        "output_name": "sim",
        "output_dataset_structure": {},
        "counter": 0,
    },
) as dag:
    gro = xcom_lookup.override(task_id="gro_from_xcom")(
        dag_id="{{ params.inputs.parent_dag_id }}",
        task_id="{{ params.inputs.gro.task_id }}",
        key="{{ params.inputs.gro.key }}",
    )
    top = xcom_lookup.override(task_id="top_from_xcom")(
        dag_id="{{ params.inputs.parent_dag_id }}",
        task_id="{{ params.inputs.top.task_id }}",
        key="{{ params.inputs.top.key }}",
    )
    mdp = xcom_lookup.override(task_id="mdp_from_xcom")(
        dag_id="{{ params.inputs.parent_dag_id }}",
        task_id="{{ params.inputs.mdp.task_id }}",
        key="{{ params.inputs.mdp.key }}",
    )

    grompp_input_list = prepare_gmx_input.override(task_id="grompp_input_list")(
        args=["grompp"],
        input_files={"-f": mdp, "-c": gro, "-p": top},
        output_files={"-o": "{{ params.output_name }}.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "iteration_{{ params.counter }}",
            "sim_",
        ],
        num_simulations="{{ params.num_simulations }}",
    )

    grompp = ResourceGmxOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list)
    grompp_input_list >> grompp

    mdrun_input_list = (
        update_gmx_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", "{{ params.output_name }}"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "{{ params.output_name }}.gro", "-dhdl": "dhdl.xvg"},
        )
        .expand(gmx_output=grompp.output)
    )
    mdrun = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
    dataset = dataset_from_xcom_dicts.override(task_id="make_dataset")(
        output_dir="{{ params.output_dir }}/iteration_{{ params.counter }}",
        output_fn="{{ params.output_name }}.json",
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun', key='return_value')}}",
        dataset_structure="{{ params.output_dataset_structure }}",
    )
    mdrun >> dataset
