import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmx_input,
    unpack_ref_t,
)
from airflowHPC.operators import ResourceBashOperator, ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from gmxapi.commandline import cli_executable
from airflow.models.param import Param


@task
def unpack_gro_inputs(**context):
    """
    It is not possible to use templating for mapped operators (e.g. calls to op.expand()).
    Thus, this task prepares gro input files for grompp.
    """
    temps_range = range(
        len(context["task"].render_template("{{ params.ref_t_list | list}}", context))
    )
    gro_input_dir = context["task"].render_template(
        "{{ params.inputs.gro.directory }}", context
    )
    step_number = context["task"].render_template("{{ params.step_number }}", context)
    input_dir = [
        f"{gro_input_dir}/iteration_{step_number}/sim_{i}" for i in temps_range
    ]
    return input_dir


@task
def unpack_cpt_inputs(**context):
    """
    It is not possible to use templating for mapped operators (e.g. calls to op.expand()).
    Thus, this task prepares cpt input files for grompp.
    """
    temps_range = range(
        len(context["task"].render_template("{{ params.ref_t_list | list}}", context))
    )
    cpt_input_dir = context["task"].render_template(
        "{{ params.inputs.cpt.directory }}", context
    )
    num_steps = context["task"].render_template("{{ params.step_number }}", context)
    input_dir = [f"{cpt_input_dir}/iteration_{num_steps}/sim_{i}" for i in temps_range]
    return input_dir


with DAG(
    dag_id="simulate_done",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": {
            "gro": {"directory": "sim", "filename": "sim.gro"},
        },
        "ref_t_list": [300, 310, 320, 330],
        "step_number": 0,
    },
) as simulate:
    simulate.doc = """Simulation of a system with replica exchange handled by mdrun -multidir option."""

    ref_temps = unpack_ref_t()
    gro_input_dirs = unpack_gro_inputs()


with DAG(
    dag_id="simulate",
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
        "ref_t_list": [300, 310, 320, 330],
        "step_number": 0,
        "output_dir": "sim",
        "expected_output": "sim.gro",
    },
) as simulate:
    simulate.doc = """Simulation of a system with replica exchange handled by mdrun -multidir option."""

    ref_temps = unpack_ref_t()
    gro_input_dirs = unpack_gro_inputs()
    cpt_input_dirs = unpack_cpt_inputs()

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
        .expand(update_dict=ref_temps)
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
            "iteration_{{ params.step_number }}",
            "sim_",
        ],
        num_simulations="{{ params.ref_t_list | length }}",
    )
    grompp_sim = ResourceGmxOperatorDataclass.partial(
        task_id="grompp_sim",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_sim)
    # This could be ResourceGmxOperator, but we use ResourceBashOperator to demo how they are equivalent
    mdrun_sim = ResourceBashOperator(
        task_id="mdrun_sim",
        bash_command=f"{cli_executable()} mdrun -replex 100 -multidir "
        + "{{ params.output_dir }}/iteration_{{ params.step_number }}/sim_[0123] -s sim.tpr -deffnm sim",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        cwd=os.path.curdir,
    )
    grompp_sim >> mdrun_sim
