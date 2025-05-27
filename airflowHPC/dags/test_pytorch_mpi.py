from airflow import DAG
from airflow.utils import timezone

from airflowHPC.operators.mpi_external_python_operator import (
    MPIExternalPythonOperator,
)


def torch_hello():
    import os
    from torch import zeros as torch_zeros
    import torch.distributed as dist

    rank = int(os.environ["MPI_LOCALRANKID"])
    size = int(os.environ["MPI_LOCALNRANKS"])
    name = os.environ["MPIR_CVAR_CH3_INTERFACE_HOSTNAME"]
    dist.init_process_group(backend="mpi")
    if rank == 0:
        tensor = torch_zeros(size)
        for i in range(1, size):
            tensor[i] = 1
            dist.send(tensor=tensor, dst=i)
            tensor[i] = 0
    else:
        tensor = torch_zeros(size)
        dist.recv(tensor=tensor, src=0)
    print(f"Rank {rank} has data {tensor} on {name}")


with DAG(
    "test_torch",
    start_date=timezone.utcnow(),
    catchup=False,
) as dag:
    single = MPIExternalPythonOperator(
        task_id="torch_hello",
        python_callable=torch_hello,
        mpi_ranks="4",
    )
