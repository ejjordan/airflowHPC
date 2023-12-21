import argparse
import os
import sys
from pathlib import Path
from airflow.utils import timezone
from airflow.models import DagBag
from airflow.configuration import conf
from airflow import settings

from airflowHPC.executors.radical_local_executor import RadicalLocalExecutor


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

dag_dir = os.path.join(scalems_airflow_dir, "airflowHPC/dags/")
dagbag = DagBag(dag_folder=dag_dir, include_examples=False, read_dags_from_db=False)

parser = argparse.ArgumentParser(description="Run a DAG")
parser.add_argument(
    "dag_id", type=str, help="DAG to run", choices=sorted(dagbag.dag_ids)
)
parser.add_argument(
    "--ncores", type=int, default=2, help="Number of cores the executor can use"
)
parser.add_argument("--log_tasks", type=bool, default=True, help="Whether to log tasks")
args = parser.parse_args()

if args.log_tasks:
    from airflowHPC.utils.task_logger import task_logger

    task_logger()

dag = dagbag.dags.get(args.dag_id)
dag.run(
    executor=RadicalLocalExecutor(parallelism=args.ncores),
    run_at_least_once=True,
    start_date=timezone.utcnow(),
)
