from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    prepare_gmx_input,
    update_gmxapi_input,
    xcom_lookup,
    dataset_from_xcom_dicts,
)
from airflowHPC.operators.resource_gmx_operator import ResourceGmxOperatorDataclass


with DAG(
    "grompp_mdrun",
    start_date=timezone.utcnow(),
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
        output_dir="{{ params.output_dir }}",
        counter=0,
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
        update_gmxapi_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", "{{ params.output_name }}", "-ntomp", "2"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "{{ params.output_name }}.gro", "-dhdl": "dhdl.xvg"},
        )
        .expand(gmxapi_output=grompp.output)
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
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.output_name }}.json",
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun', key='return_value')}}",
        dataset_structure="{{ params.output_dataset_structure }}",
    )
    mdrun >> dataset
