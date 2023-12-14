script_parent_dir=$(dirname $(dirname $(readlink -f "$0")))
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"
export PYTHONPATH="$script_parent_dir/airflowHPC/executors/"
#export AIRFLOW__CORE__EXECUTOR=zmq_local_executor.ZmqLocalExecutor
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$script_parent_dir/airflowHPC/airflowHPC/dags/"
airflow standalone
