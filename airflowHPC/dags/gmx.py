from airflow import DAG
from airflow.utils.timezone import datetime
from airflowHPC.dags.tasks import get_file
from airflowHPC.operators import ResourceGmxOperator

with DAG(
    "run_gmx",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={"output_dir": "run_gmx"},
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
        input_files={"-f": input_mdp, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    """
    from airflowHPC.operators.mpi_gmx_bash_operator import MPIGmxBashOperator
    mdrun_result = MPIGmxBashOperator(
        task_id="mdrun",
        mpi_ranks=4,
        cpus_per_task=2,
        gmx_arguments=["mdrun", "-ntomp", "2"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
    """
    mdrun_result = ResourceGmxOperator(
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
        output_dir="{{ params.output_dir }}",
    )
    grompp_result >> mdrun_result
