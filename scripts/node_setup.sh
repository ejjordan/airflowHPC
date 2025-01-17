PREFIX=/cfs/klemming/projects/supr/scale-ms
source $PREFIX/spack/share/spack/setup-env.sh
spack load postgresql
source $PREFIX/python_venv/bin/activate
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$PREFIX/airflowHPC/airflowHPC/dags/"
export AIRFLOW__HPC__CORES_PER_NODE=64
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
export AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=False
export RADICAL_UTILS_NO_ATFORK=1

# Handle case where non-default port number is used
portnum=$(grep -oP "^port = \K\d+" $PREFIX/postgresql_db/data/postgresql.conf)
if [ -n "$portnum" ]; then
  echo "Port number is $portnum"
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost:$portnum/airflow_db"
  pg_opts="-o \"-p $portnum\" -D $PREFIX/postgresql_db/data/ -l $PREFIX/postgresql_db/server.log"
else
  echo "Port number not found in postgresql_db/data/postgresql.conf"
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
  pg_opts="-D $PREFIX/postgresql_db/data/ -l $PREFIX/postgresql_db/server.log"
fi

total_cores=$SLURM_NTASKS*$SLURM_CPUS_PER_TASK

#status=$(pg_ctl -D $PREFIX/postgresql_db/data/ -l $PREFIX/postgresql_db/server.log status)
status_cmd="pg_ctl $pg_opts status"
start_cmd="pg_ctl $pg_opts start"
status=$(eval $status_cmd)
if [[ "$status" == *"no server running"* ]]; then
  echo "Starting PostgreSQL server"
  #pg_ctl -D $PREFIX/postgresql_db/data/ -l $PREFIX/postgresql_db/server.log start
  eval $start_cmd
else
  echo "PostgreSQL server is already running"
fi

airflow pools set default_pool 512 "default"
