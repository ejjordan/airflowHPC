from airflow import DAG
from airflowHPC.operators import ResourceBashOperator
from airflow.utils import timezone


with DAG(
    "rct_test_resources_multi",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    ResourceBashOperator.partial(
        task_id="sleep",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 4,
            "gpu_type": "rocm",
        },
    ).expand(bash_command=["sleep 5", "sleep 6"])
