import os
from airflow import DAG
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmx_input,
    unpack_mdp_options,
    unpack_inputs,
)
from airflowHPC.operators import ResourceBashOperator, ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.models.param import Param

try:
    from gmxapi.commandline import cli_executable

    gmx_executable = cli_executable()
except:
    gmx_executable = "gmx_mpi"


with DAG(
    dag_id="simulate_multidir",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
                "gro": {"directory": "npt_equil", "filename": "npt.gro"},
                "cpt": {"directory": "npt_equil", "filename": "npt.cpt"},
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
                "required": ["mdp", "gro", "cpt", "top"],
            },
            section="inputs",
        ),
        "mdp_options": [{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}],
        "output_dir": "sim",
        "expected_output": "sim.gro",
    },
) as simulate:
    simulate.doc = """Simulation of a system with replica exchange handled by mdrun -multidir option."""

    mdp_options = unpack_mdp_options()
    gro_input_dirs = unpack_inputs.override(task_id="unpack_gro_inputs")(
        param_string="{{ params.inputs.gro.directory }}"
    )
    cpt_input_dirs = unpack_inputs.override(task_id="unpack_cpt_inputs")(
        param_string="{{ params.inputs.cpt.directory }}"
    )

    top_sim = get_file.override(task_id="get_sim_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro_sim = (
        get_file.override(task_id="get_sim_gro")
        .partial(file_name="{{ params.inputs.gro.filename }}", use_ref_data=False)
        .expand(input_dir=gro_input_dirs)
    )
    cpt_sim = (
        get_file.override(task_id="get_sim_cpt")
        .partial(file_name="{{ params.inputs.cpt.filename }}", use_ref_data=False)
        .expand(input_dir=cpt_input_dirs)
    )
    mdp_json_sim = get_file.override(task_id="get_sim_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_sim = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp_sim")
        .partial(mdp_json_file_path=mdp_json_sim)
        .expand(update_dict=mdp_options)
    )
    grompp_input_list_sim = prepare_gmx_input(
        args=["grompp"],
        input_files={
            "-f": mdp_sim,
            "-c": gro_sim,
            "-r": gro_sim,
            "-p": top_sim,
            "-t": cpt_sim,
        },
        output_files={"-o": "sim.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "sim_",
        ],
        num_simulations="{{ params.mdp_options | length }}",
    )
    grompp_sim = ResourceGmxOperatorDataclass.partial(
        task_id="grompp_sim",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_sim)
    # This could be ResourceGmxOperator, but this shows how ResourceBashOperator can be equivalent
    mdrun_sim = ResourceBashOperator(
        task_id="mdrun_sim",
        bash_command=f"{gmx_executable} mdrun -replex 100 -multidir "
        + "{{ params.output_dir }}/sim_[0123] -s sim.tpr -deffnm sim",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        cwd=os.path.curdir,
    )
    grompp_sim >> mdrun_sim
