import tempfile
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflowHPC.operators import ResourceBashOperator
from airflowHPC.dags.tasks import get_file


with DAG(
    "test_mpi_omp",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    fn = "hello_mpi_omp"
    get_code = get_file.override(task_id="get_code")(
        input_dir="mpi", file_name=f"{fn}.c"
    )
    tempdir = PythonOperator(
        task_id="tempdir",
        python_callable=lambda: tempfile.mkdtemp(),
    )
    compile_code = BashOperator(
        task_id="compile_code",
        bash_command=f"mpicc -fopenmp -o {fn} {get_code}",
        cwd="{{ task_instance.xcom_pull(task_ids='tempdir') }}",
    )
    compile_code.template_fields = ("bash_command", "cwd")
    """
    from airflowHPC.operators.mpi_bash_operator import MPIBashOperator
    run_code = MPIBashOperator(
        task_id="hello_mpi_omp",
        bash_command="./hello_mpi_omp",
        mpi_ranks=4,
        cpus_per_task=2,
        cwd="{{ task_instance.xcom_pull(task_ids='tempdir') }}",
    )
    """
    run_code = ResourceBashOperator(
        task_id="hello_mpi_omp",
        bash_command="./hello_mpi_omp",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        cwd="{{ task_instance.xcom_pull(task_ids='tempdir') }}",
    )
    run_code.template_fields = ("cwd",)
    [get_code, tempdir] >> compile_code >> run_code
