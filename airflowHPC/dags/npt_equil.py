from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmx_input,
    update_gmx_input,
    unpack_mdp_options,
)
from airflowHPC.operators import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "npt_equil",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "npt.json"},
                "gro": {"directory": "nvt_equil", "filename": "nvt.gro"},
                "top": {"directory": "prep", "filename": "system_prepared.top"},
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
        "output_dir": "npt_equil",
        "expected_output": "npt.gro",
        "mdp_options": [{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}],
        "step_number": 0,
    },
) as npt_equil:
    npt_equil.doc = """NPT equilibration."""

    mdp_json_npt = get_file.override(task_id="get_npt_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    ref_temps = unpack_mdp_options()
    mdp_npt = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_npt_update")
        .partial(mdp_json_file_path=mdp_json_npt)
        .expand(update_dict=ref_temps)
    )
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
    )
    grompp_input_list_npt = prepare_gmx_input(
        args=["grompp"],
        input_files={
            "-f": mdp_npt,
            "-c": gro,
            "-r": gro,
            "-p": top,
        },
        output_files={"-o": "npt.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "iteration_{{ params.step_number }}",
            "sim_",
        ],
        num_simulations="{{ params.ref_t_list | length }}",
    )
    grompp_npt = ResourceGmxOperatorDataclass.partial(
        task_id="grompp_npt",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_npt)
    mdrun_input = (
        update_gmx_input.override(task_id="mdrun_prepare_npt")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-e": "ener.edr",
                "-c": "{{ params.expected_output }}",
                "-x": "npt.xtc",
                "-cpo": "npt.cpt",
            },
        )
        .expand(gmx_output=grompp_npt.output)
    )
    mdrun_result = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun_npt",
        executor_config={
            "mpi_ranks": 3,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input)
