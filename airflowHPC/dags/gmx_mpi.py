from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.operators.radical_gmxapi_bash_operator import RadicalGmxapiBashOperator

with DAG(
    "run_gmxapi_mpi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={"output_dir": "outputs"},
) as run_gmxapi_mpi:
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

    mdrun_result = RadicalGmxapiBashOperator(
        task_id="mdrun",
        arguments=["mdrun"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
        executor_config={
            "RadicalExecutor": {
                "use_mpi": True,
                "ranks": 4,
                "cores_per_rank": 2,
            }
        },
    )
    grompp_result >> mdrun_result
