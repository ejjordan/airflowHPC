script_parent_dir=$(dirname $(dirname $(readlink -f "$0")))
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.radical_local_executor.RadicalLocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$script_parent_dir/airflowHPC/dags/"
airflow scheduler
