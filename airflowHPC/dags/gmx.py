from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi, _run_gmxapi
from airflowHPC.operators.radical_operator import RadicalOperator


def _gmxapi(
    args: list, input_files: dict, output_files: dict, output_dir: str, stdin=None
):
    gmx = _run_gmxapi(args, input_files, output_files, output_dir, stdin)
    return {f"{key}": f"{gmx.output.file[key].result()}" for key in output_files.keys()}


with DAG(
    "run_gmxapi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={"output_dir": "outputs"},
) as dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="ensemble_md", file_name="expanded.mdp"
    )
    grompp_result = run_gmxapi.override(task_id="grompp")(
        args=["grompp"],
        input_files={"-f": input_mdp, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_result = RadicalOperator(
        task_id="mdrun",
        python_callable=_gmxapi,
        op_args=["mdrun"],
        op_kwargs={
            "input_files": {"-s": grompp_result["-o"]},
            "output_files": {"-c": "result.gro", "-x": "result.xtc"},
            "output_dir": "{{ params.output_dir }}",
        },
        queue="radical",
        executor_config={
            "RadicalExecutor": {
                "use_mpi": True,
                "ranks": 1,
                "cores_per_rank": 2,
            }
        },
    )
    """
    mdrun_result = run_gmxapi.override(task_id="mdrun")(
        args=["mdrun"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
    """
