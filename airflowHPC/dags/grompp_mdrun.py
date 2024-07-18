from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    update_gmxapi_input,
    xcom_lookup,
    dataset_from_xcom_dicts,
)
from airflowHPC.operators.resource_gmx_operator import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "grompp_mdrun",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "min.json"},
                "gro_task_id": "solvate",
                "top_task_id": "solvate",
                "parent_dag_id": "alanine_dipeptide",
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
    },
) as dag:
    mdp_json = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_update")(
        mdp_json_file_path=mdp_json
    )

    gros = xcom_lookup.override(task_id="gro_from_xcom")(
        dag_id="{{ params.inputs.parent_dag_id }}",
        task_id="{{ params.inputs.gro_task_id }}",
        key="-o",
    )
    tops = xcom_lookup.override(task_id="top_from_xcom")(
        dag_id="{{ params.inputs.parent_dag_id }}",
        task_id="{{ params.inputs.top_task_id }}",
        key="-p",
    )

    grompp_input_list = prepare_gmxapi_input.override(task_id="grompp_input_list")(
        args=["grompp"],
        input_files={"-f": mdp, "-c": gros, "-p": tops},
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
            output_files={"-c": "{{ params.output_name }}.gro"},
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

    gro_dataset = dataset_from_xcom_dicts.override(task_id="gro_dataset")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.output_name }}.json",
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun', key='return_value')}}",
        key="-c",
    )
    mdrun >> gro_dataset
