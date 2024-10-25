import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    unpack_ref_t,
)
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from gmxapi.commandline import cli_executable
from airflow.models.param import Param

try:
    gmx_executable = cli_executable()
except:
    gmx_executable = "gmx_mpi"


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
    dag_id="simulate_cpt",
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
        "output_dir": "sim",
        "expected_output": "sim.gro",
    },
) as simulate_cpt:
    simulate_cpt.doc = """Simulation from a cpt file."""

    top_cpt = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro_cpt = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.gro.directory }}",
    )
    cpt_cpt = get_file.override(task_id="get_cpt")(
        file_name="{{ params.inputs.cpt.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.cpt.directory }}",
    )
    mdp_json_cpt = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_cpt = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")(
        mdp_json_file_path=mdp_json_cpt
    )

    grompp_cpt = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={
            "-f": mdp_cpt,
            "-c": gro_cpt,
            "-r": gro_cpt,
            "-p": top_cpt,
            "-t": cpt_cpt,
        },
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_cpt = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-e": "ener.edr",
            "-c": "{{ params.expected_output }}",
            "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
            "-cpo": "{{ params.expected_output | replace('.gro', '.cpt') }}",
        },
        output_dir="{{ params.output_dir }}",
    )
    grompp_cpt >> mdrun_cpt


with DAG(
    dag_id="simulate_no_cpt",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
                "gro": {"directory": "npt_equil", "filename": "npt.gro"},
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
        "output_dir": "sim",
        "expected_output": "sim.gro",
    },
) as simulate_no_cpt:
    simulate_no_cpt.doc = """Simulation without a cpt file."""

    top_no_cpt = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro_no_cpt = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.gro.directory }}",
    )
    mdp_json_no_cpt = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_no_cpt = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")(
        mdp_json_file_path=mdp_json_no_cpt
    )

    grompp_no_cpt = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={
            "-f": mdp_no_cpt,
            "-c": gro_no_cpt,
            "-r": gro_no_cpt,
            "-p": top_no_cpt,
        },
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_no_cpt = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-e": "ener.edr",
            "-c": "{{ params.expected_output }}",
            "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
            "-cpo": "{{ params.expected_output | replace('.gro', '.cpt') }}",
        },
        output_dir="{{ params.output_dir }}",
    )
    grompp_no_cpt >> mdrun_no_cpt
