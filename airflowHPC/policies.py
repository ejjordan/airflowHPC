from airflow.policies import hookimpl
from airflow.models.baseoperator import BaseOperator


@hookimpl
def task_policy(task: BaseOperator):
    if task.operator_name == "RadicalBashOperator":
        if task.pool is None or task.pool == "default_pool":
            task.pool = "mpi_pool"
    if task.operator_name == "RadicalGmxapiBashOperator":
        if task.pool is None or task.pool == "default_pool":
            task.pool = "mpi_pool"
    if task.operator_name == "RadicalExternalPythonOperator":
        if task.pool is None or task.pool == "default_pool":
            task.pool = "mpi_pool"
