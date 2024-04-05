#include <stdio.h>
#include <mpi.h>
#include <omp.h>

int main(int argc, char *argv[]) {
  int mpi_rank, omp_thread;
  int provided, required=MPI_THREAD_FUNNELED;
  MPI_Init_thread(&argc, &argv, required, &provided);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
#pragma omp parallel private(omp_thread)
  {
    omp_thread = omp_get_thread_num();
    printf("Hello from MPI rank %d, OMP thread %d\n", mpi_rank, omp_thread);
  }
  MPI_Finalize();
}
