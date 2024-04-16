from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.operators.radical_gmxapi_bash_operator import RadicalGmxapiBashOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


@task
def outputs_list(**context):
    num_sims = int(context["task"].render_template("{{ params.num_sims }}", context))
    output_dir = context["task"].render_template("{{ params.output_dir }}", context)
    return [f"{output_dir}/sim_{i}" for i in range(num_sims)]


with DAG(
    "run_gmxapi_multi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={
        "output_dir": "outputs",
        "num_sims": 4,
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "basic_md.json"},
            "gro": {"directory": "ensemble_md", "filename": "sys.gro"},
            "top": {"directory": "ensemble_md", "filename": "sys.top"},
        },
    },
) as gmxapi_multi:
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
        update_dict={"nsteps": 25000},
    )
    grompp_result = run_gmxapi.override(task_id="grompp")(
        args=["grompp"],
        input_files={"-f": mdp_sim, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    outputs_dirs = outputs_list.override(task_id="get_output_dirs")()
    mdrun_result = RadicalGmxapiBashOperator.partial(
        task_id="mdrun",
        mpi_ranks=4,
        cpus_per_task=2,
        gmx_arguments=["mdrun", "-ntomp", "2"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
    ).expand(output_dir=outputs_dirs)
    grompp_result >> mdrun_result
