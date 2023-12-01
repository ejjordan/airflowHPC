from __future__ import annotations

from airflow.auth.managers.fab.models import Permission, Resource, assoc_permission_role
from airflow.jobs.job import Job
from airflow.models import (
    Connection,
    DagModel,
    DagRun,
    DagTag,
    DbCallbackRequest,
    Log,
    Pool,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
    XCom,
    errors,
)
from airflow.models.dag import DagOwnerAttributes
from airflow.models.dagcode import DagCode
from airflow.models.dagwarning import DagWarning
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetDagRunQueue,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.db import (
    add_default_pool_if_not_exists,
    create_default_connections,
    reflect_tables,
)
from airflow.utils.session import create_session


def clear_db_runs():
    with create_session() as session:
        session.query(Job).delete()
        session.query(Trigger).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


def clear_db_datasets():
    with create_session() as session:
        session.query(DatasetEvent).delete()
        session.query(DatasetModel).delete()
        session.query(DatasetDagRunQueue).delete()
        session.query(DagScheduleDatasetReference).delete()
        session.query(TaskOutletDatasetReference).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagOwnerAttributes).delete()
        session.query(DagModel).delete()


def clear_db_dag_code():
    with create_session() as session:
        session.query(DagCode).delete()


def clear_db_xcom():
    with create_session() as session:
        session.query(XCom).delete()


def clear_db_jobs():
    with create_session() as session:
        session.query(Job).delete()


def clear_db_task_fail():
    with create_session() as session:
        session.query(TaskFail).delete()


def clear_db_logs():
    with create_session() as session:
        session.query(Log).delete()


def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_datasets()
    clear_db_dags()
    clear_db_dag_code()
    clear_db_xcom()
    clear_db_jobs()
    clear_db_task_fail()
    clear_db_logs()


clean_database()
