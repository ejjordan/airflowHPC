from airflow import DAG
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmx_input,
    unpack_mdp_options,
    update_gmx_input,
    unpack_param,
)
from airflowHPC.operators import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.models.param import Param


with DAG(
    dag_id="anthracene",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "anthracene", "filename": "dg_mdp.json"},
                "gro": {
                    "directory": "anthracene/gro",
                    "filename": [
                        "lam00.gro",
                        "lam01.gro",
                        "lam02.gro",
                        "lam03.gro",
                        "lam04.gro",
                        "lam05.gro",
                        "lam06.gro",
                        "lam07.gro",
                        "lam08.gro",
                        "lam09.gro",
                        "lam10.gro",
                    ],
                },
                "top": {"directory": "anthracene", "filename": "anthracene.top"},
            },
            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
                "properties": {
                    "mdp": {"type": ["object", "null"]},
                    "gro": {"type": ["object", "null"]},
                    "top": {"type": ["object", "null"]},
                },
                "required": ["mdp", "gro", "top"],
            },
            section="inputs",
        ),
        "mdp_options": [
            {"init-lambda-state": 0},
            {"init-lambda-state": 10},
            {"init-lambda-state": 20},
            {"init-lambda-state": 30},
            {"init-lambda-state": 40},
            {"init-lambda-state": 50},
            {"init-lambda-state": 60},
            {"init-lambda-state": 70},
            {"init-lambda-state": 80},
            {"init-lambda-state": 90},
            {"init-lambda-state": 100},
        ],
        "output_dir": "anthracene",
        "output_name": "anthra",
    },
) as simulate:
    mdp_options = unpack_mdp_options()
    gro_files = unpack_param.override(task_id="get_gro_files")(
        "{{ params.inputs.gro.filename }}"
    )

    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=True,
    )
    gro = (
        get_file.override(task_id="get_gro")
        .partial(input_dir="{{ params.inputs.gro.directory }}", use_ref_data=True)
        .expand(file_name=gro_files)
    )
    mdp_json = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")
        .partial(mdp_json_file_path=mdp_json)
        .expand(update_dict=mdp_options)
    )
    grompp_input_list = prepare_gmx_input(
        args=["grompp"],
        input_files={
            "-f": mdp,
            "-c": gro,
            "-r": gro,
            "-p": top,
        },
        output_files={"-o": "sim.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "sim_",
        ],
        num_simulations="{{ params.mdp_options | length }}",
    )
    grompp = ResourceGmxOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list)
    grompp_input_list >> grompp

    mdrun_input_list = (
        update_gmx_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "{{ params.output_name }}.gro", "-dhdl": "dhdl.xvg"},
        )
        .expand(gmx_output=grompp.output)
    )
    mdrun = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 4,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
