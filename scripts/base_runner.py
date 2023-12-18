import os
import sys
from pathlib import Path
from airflow.utils import timezone
from airflow.models import DagBag

# from loud_logger import loud_logger

from airflow.configuration import conf
from airflow import settings

from airflowHPC.executors.zmq_local_executor import ZmqLocalExecutor

conf.set(
    "database",
    "sql_alchemy_conn",
    "mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db",
)

os.environ["AIRFLOW__CORE__PARALLELISM"] = "2"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = "~/dev/airflowHPC/airflowHPC/dags/"

scalems_airflow_dir = os.path.dirname(Path(__file__).parent)
sys.path.append(scalems_airflow_dir)
# Both of these seem to be needed to flush the config changes before changing the executor
settings.configure_vars()
settings.configure_orm()
# sys.path.append(os.path.join(scalems_airflow_dir, 'executors/'))
# conf.set('core', 'executor', "executors.radical_executor.RadicalExecutor")

'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db"'
"export PYTHONPATH=~/dev/airflowHPC/airflowHPC/executors/"
"export AIRFLOW__CORE__EXECUTOR=zmq_local_executor.ZmqLocalExecutor"
"export AIRFLOW__CORE__PARALLELISM=2"
"export AIRFLOW__CORE__DAGS_FOLDER=~/dev/airflowHPC/airflowHPC/dags/"
"export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=600"  # for use with DebugExecutor
"export AIRFLOW__CORE__LOAD_EXAMPLES=False"

# loud_logger()

dag_dir = os.path.join(scalems_airflow_dir, "airflowHPC/dags/")

utc_now = timezone.utcnow()

dagbag = DagBag(dag_folder=dag_dir, include_examples=False, read_dags_from_db=False)
dag = dagbag.dags.get("get_source")
dag.run(executor=ZmqLocalExecutor(), run_at_least_once=True, start_date=utc_now)
# import ipdb; ipdb.set_trace()
