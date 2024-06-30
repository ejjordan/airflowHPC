export AirflowHPC_dir=$(dirname -- "$( dirname -- "${BASH_SOURCE[0]}")")
if [ -z "$1" ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
elif [ "$1" == "postgres" ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
elif [ "$1" == "mysql" ]; then
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"
else
  echo "Please select either 'postgres' or 'mysql' database backend."
   echo "Chose: $1"
  exit 1
fi
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$AirflowHPC_dir/airflowHPC/dags
export AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW="graph"
# Silence an annoying warning from radical.utils
export RADICAL_UTILS_NO_ATFORK=1
airflow standalone
