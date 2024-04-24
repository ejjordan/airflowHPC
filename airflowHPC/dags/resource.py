from airflow import DAG
from airflowHPC.operators.radical_bash_operator import RadicalBashOperator
from airflow.utils import timezone


with DAG(
    "test_resources",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    s1 = RadicalBashOperator(
        task_id="sleep_1",
        bash_command="sleep 5",
        # executor_config={"mpi_ranks": 8, "gpus": 2},
        mpi_ranks=8,
        cpus_per_task=2,
        gpus=4,
    )
    s2 = RadicalBashOperator(
        task_id="sleep_2",
        bash_command="sleep 4",
        # executor_config={"mpi_ranks": 8, "gpus": 2},
        mpi_ranks=8,
        cpus_per_task=2,
        gpus=2,
    )
