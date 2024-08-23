import argparse
import os
import sys
from pathlib import Path
from airflow.utils import timezone
from airflow.models import DagBag
from airflow.configuration import conf
from airflow import settings

from airflowHPC.executors.radical_executor import RadicalExecutor


conf.set(
    "database",
    "sql_alchemy_conn",
    "mysql+mysqldb://airflow_user:airflow_pass@localhost/airflow_db",
)

os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = "~/dev/airflowHPC/airflowHPC/dags/"

scalems_airflow_dir = os.path.dirname(Path(__file__).parent)
sys.path.append(scalems_airflow_dir)
# Both of these seem to be needed to flush the config changes before changing the executor
settings.configure_vars()
settings.configure_orm()

# The start_date must be before the start_date of the DAG
start_date = timezone.utcnow()
dag_dir = os.path.join(scalems_airflow_dir, "airflowHPC/dags/")
dagbag = DagBag(dag_folder=dag_dir, include_examples=False, read_dags_from_db=False)

parser = argparse.ArgumentParser(description="Run a DAG")
parser.add_argument(
    "dag_id", type=str, help="DAG to run", choices=sorted(dagbag.dag_ids)
)
parser.add_argument(
    "-n", "--ncores", type=int, default=4, help="Number of cores the executor can use"
)
parser.add_argument(
    "-ntl", "--no_task_log", action="store_false", help="Turns off task logging"
)
args = parser.parse_args()

if args.no_task_log:
    from airflowHPC.utils.task_logger import task_logger

    task_logger()

dag = dagbag.dags.get(args.dag_id)
if "schedule" in dag.tags:
    """The replica_exchange DAG does not work well with dag.run so use the scheduler to launch it.
    This unfortunately means that the logging is quite limited, so the other dags are probably
    better for development purposes."""
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
    from airflow.jobs.job import Job, run_job

    scheduler_job = Job(
        executor=RadicalLocalExecutor(parallelism=args.ncores),
        job_type=SchedulerJobRunner.job_type,
        start_date=start_date,
        dag_id=args.dag_id,
    )
    job_runner = SchedulerJobRunner(job=scheduler_job, subdir=dag_dir, num_runs=1)
    run_job(job=job_runner.job, execute_callable=job_runner._execute)
else:
    dag.run(
        executor=RadicalLocalExecutor(parallelism=args.ncores),
        run_at_least_once=True,
        start_date=start_date,
    )
