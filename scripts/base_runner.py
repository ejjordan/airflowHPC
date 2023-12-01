import os
import sys
from airflow.utils import timezone
from airflow.models import DagBag

# from loud_logger import loud_logger

from airflow.configuration import conf
from airflow import settings

conf.set(
    "database",
    "sql_alchemy_conn",
    "mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db",
)
scalems_airflow_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(scalems_airflow_dir)
# Both of these seem to be needed to flush the config changes before changing the executor
settings.configure_vars()
settings.configure_orm()
# sys.path.append(os.path.join(scalems_airflow_dir, 'executors/'))
# conf.set('core', 'executor', "executors.radical_executor.RadicalExecutor")

'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"'
"export PYTHONPATH=/home/joe/dev/airflow-hpc/executors/"
"export AIRFLOW__CORE__EXECUTOR=zmq_local_executor.ZmqLocalExecutor"
"export AIRFLOW__CORE__PARALLELISM=2"
"export AIRFLOW__CORE__DAGS_FOLDER=/home/joe/dev/airflow-hpc/dags/"
"export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=600"  # for use with DebugExecutor
"export AIRFLOW__CORE__LOAD_EXAMPLES=False"

# loud_logger()

dag_dir = os.path.join(scalems_airflow_dir, "dags/")

utc_now = DEFAULT_DATE = timezone.utcnow()

dagbag = DagBag(dag_folder=dag_dir, include_examples=False, read_dags_from_db=False)
dag = dagbag.dags.get("looper")
dag.run()
# import ipdb; ipdb.set_trace()
