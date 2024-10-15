from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file



@task
def outputs_list(dir_name: str = "", sims = None, **context):
    if sims:
        num_sims = sims
    else:
        num_sims = int(context["task"].render_template("{{ params.num_sims }}", context))
    output_dir = context["task"].render_template("{{ params.output_dir }}", context)
    return [f"{output_dir}/{dir_name}/sim_{i}" for i in range(num_sims)]


with DAG(
    "gmxapi_multi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={
        "output_dir": "outputs",
        "num_sims": 2,
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "basic_md.json"},
            "gro": {"directory": "ensemble_md", "filename": "sys.gro"},
            "top": {"directory": "ensemble_md", "filename": "sys.top"},
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
    mdp_sim1 = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update1")(
        mdp_json_file_path=input_mdp,
        update_dict={"nsteps": 5000},
    )
    grompp_batch_1 = run_gmxapi.override(task_id="grompp_batch_1")(
        args=["grompp"],
        input_files={"-f": mdp_sim1, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}" + "/batch_1",
    )
    mdrun_batch_1 = run_gmxapi.override(task_id="mdrun_batch_1")(
        args=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp_batch_1')['-o'] }}"},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}" + "/batch_1",
    )
    mdp_sim2 = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update2")(
        mdp_json_file_path=input_mdp,
        update_dict={"nsteps": 100},
    )
    grompp_batch_2 = run_gmxapi.override(task_id="grompp_batch_2")(
        args=["grompp"],
        input_files={"-f": mdp_sim2, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}" + "/batch_2",
    )
    outputs_dirs_2 = outputs_list.override(task_id="get_output_dirs_2")("batch_2")
    mdrun_batch_2 = run_gmxapi.override(task_id="mdrun_batch_2").partial(
        args=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp_batch_2')['-o'] }}"},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
    ).expand(output_dir=outputs_dirs_2)
    grompp_batch_1 >> mdrun_batch_1 >> mdrun_batch_2
    grompp_batch_2 >> mdrun_batch_2
