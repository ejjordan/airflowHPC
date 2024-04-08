from airflow import DAG
from airflow.utils import timezone

from airflowHPC.operators.radical_external_python_operator import (
    RadicalExternalPythonOperator,
)


def mpi4py_hello():
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    name = MPI.Get_processor_name()
    print(f"Hello from rank {rank} of {size} on {name}")


with DAG(
    "test_mpi4py",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    RadicalExternalPythonOperator(
        task_id="mpi4py_hello",
        python_callable=mpi4py_hello,
        mpi_ranks="4",
    )
    RadicalExternalPythonOperator.partial(
        task_id="mpi4py_hello_multi", python_callable=mpi4py_hello
    ).expand(mpi_ranks=["4", "8"])
