from airflow import DAG
from airflow.decorators import task

from airflowHPC.dags.tasks import get_file
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.utils.timezone import datetime


@task
def outputs_list(**context):
    num_sims = int(context["task"].render_template("{{ params.num_sims }}", context))
    output_dir = context["task"].render_template("{{ params.output_dir }}", context)
    return [f"{output_dir}/sim_{i}" for i in range(num_sims)]


with DAG(
    "gmx_multi",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "output_dir": "gmx_multi",
        "num_sims": 4,
        "mdp_options": {"nsteps": 10000},
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "sim.json"},
            "gro": {
                "directory": "ala_pentapeptide",
                "filename": "ala_penta_capped_solv.gro",
            },
            "top": {
                "directory": "ala_pentapeptide",
                "filename": "ala_penta_capped_solv.top",
            },
        },
    },
) as gmx_multi:
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
    mdp_sim = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update")(
        mdp_json_file_path=input_mdp,
        update_dict="{{ params.mdp_options }}",
    )
    grompp_result = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={"-f": mdp_sim, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    outputs_dirs = outputs_list.override(task_id="get_output_dirs")()
    mdrun_result = ResourceGmxOperator.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
    ).expand(output_dir=outputs_dirs)
    grompp_result >> mdrun_result
