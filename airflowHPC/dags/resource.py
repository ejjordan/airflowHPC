from airflow import DAG
from airflowHPC.operators.resource_bash_operator import ResourceBashOperator
from airflow.utils import timezone


with DAG(
    "test_resources",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    ResourceBashOperator(
        task_id="sleep_1",
        bash_command="sleep 5",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 4,
            "gpu_type": "nvidia",
        },
    )
    ResourceBashOperator(
        task_id="sleep_2",
        bash_command="sleep 5",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 2,
            "gpu_type": "nvidia",
        },
    )
