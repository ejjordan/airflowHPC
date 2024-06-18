from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.operators.mpi_gmx_bash_operator import MPIGmxBashOperator

with DAG(
    "run_gmx",
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
    mdrun_result = MPIGmxBashOperator(
        task_id="mdrun",
        mpi_ranks=4,
        cpus_per_task=2,
        gmx_arguments=["mdrun", "-ntomp", "2"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
