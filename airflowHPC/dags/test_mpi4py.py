from airflow import DAG
from airflow.utils import timezone

from airflowHPC.operators.radical_external_python_operator import (
    RadicalExternalPythonOperator,
)


def mpi4py_hello(delay):
    from time import sleep
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    name = MPI.Get_processor_name()
    sleep(delay)
    print(f"Hello from rank {rank} of {size} on {name}")
    return {"last_rank": rank}


with DAG(
    "test_mpi4py",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    single = RadicalExternalPythonOperator(
        task_id="mpi4py_hello",
        python_callable=mpi4py_hello,
        mpi_ranks="4",
        op_args=[1],
    )
    multi = RadicalExternalPythonOperator.partial(
        task_id="mpi4py_op_args", python_callable=mpi4py_hello, mpi_ranks="8"
    ).expand(op_kwargs=[{"delay": 1}, {"delay": 2}, {"delay": 3}])

    single >> multi

    """Can't map mpi_ranks because it is used to set unmappable pool_slots arg
    RadicalExternalPythonOperator.partial(
        task_id="mpi4py_expand_mpi", python_callable=mpi4py_hello, op_kwargs={"delay": 1}
    ).expand(mpi_ranks=["4", "8"])"""
