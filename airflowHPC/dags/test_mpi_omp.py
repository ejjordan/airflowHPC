import tempfile
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflowHPC.operators.radical_gmxapi_bash_operator import RadicalBashOperator
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
    compile_code.template_fields = ("bash_command", "env", "cwd")
    mdrun_result = RadicalBashOperator(
        task_id="hello_mpi_omp",
        bash_command="./hello_mpi_omp",
        mpi_executable="mpirun",
        mpi_ranks="4",
        cpus_per_task="2",
        cwd="{{ task_instance.xcom_pull(task_ids='tempdir') }}",
    )
    mdrun_result.template_fields = ("bash_command", "env", "cwd")
    [get_code, tempdir] >> compile_code >> mdrun_result
