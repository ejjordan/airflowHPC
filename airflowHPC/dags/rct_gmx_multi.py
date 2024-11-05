from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, prepare_gmx_input, update_gmx_input
from airflowHPC.operators import ResourceRCTOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "rct_multi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={
        "output_dir": "rct_multi",
        "num_simulations": 4,
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "basic_md.json"},
            "gro": {"directory": "ensemble_md", "filename": "sys.gro"},
            "top": {"directory": "ensemble_md", "filename": "sys.top"},
        },
    },
) as rct_multi:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )

    mdp_sim = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=input_mdp
    ).expand(
        update_dict=[
            {"nsteps": 5000},
            {"nsteps": 5000},
            {"nsteps": 5000},
            {"nsteps": 5000},  # , "dt": 0.2},
            # < 0.002 fs: expected to succeed
            # > 0.1   fs: expected to fail
        ]
    )

    grompp_input_list_sim = prepare_gmx_input.override(task_id="grompp_input_list")(
        args=["grompp", "-maxwarn", "1"],
        input_files={
            "-f": mdp_sim,
            "-c": input_gro,
            "-p": input_top,
        },
        output_files={"-o": "run.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "sim_",
        ],
        num_simulations="{{ params.num_simulations }}",
    )

    grompp_result = ResourceRCTOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_sim)
    grompp_input_list_sim >> grompp_result

    mdrun_input_list = (
        update_gmx_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", "result", "-ntomp", "2"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "result.gro", "-x": "result.xtc"},
        )
        .expand(gmx_output=grompp_result.output)
    )
    mdrun_result = ResourceRCTOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
    grompp_result >> mdrun_input_list >> mdrun_result
