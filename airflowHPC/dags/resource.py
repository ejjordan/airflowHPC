from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone


with DAG(
    "test_resources",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    s1 = BashOperator(
        task_id="sleep_1",
        bash_command="sleep 5",
        executor_config={"mpi_ranks": 8, "gpu_per_rank": 2},
    )
    s2 = BashOperator(
        task_id="sleep_2",
        bash_command="sleep 10",
        executor_config={"mpi_ranks": 8, "gpu_per_rank": 2},
    )
    s3 = BashOperator(
        task_id="sleep_3",
        bash_command="sleep 10",
        executor_config={"mpi_ranks": 8, "gpu_per_rank": 2},
    )
    s4 = BashOperator(
        task_id="sleep_4",
        bash_command="sleep 10",
        executor_config={"mpi_ranks": 8, "gpu_per_rank": 2},
    )

    s1 >> [s2, s3, s4]
