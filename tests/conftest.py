import pytest
import os

from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from slugify import slugify
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.session import create_session
from airflow.jobs.job import Job
from airflow.models import DagRun, TaskInstance, Trigger
from airflow.utils import timezone
from contextlib import suppress

data_path = os.path.join(os.path.dirname(__file__), "data")

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def clear_db_runs():
    with create_session() as session:
        session.query(Job).delete()
        session.query(Trigger).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()

@pytest.fixture
def session():
    from airflow.utils.session import create_session

    with create_session() as session:
        yield session
        session.rollback()


@pytest.fixture
def dag_maker(request):
    """Fixture to help create DAG, DagModel, and SerializedDAG automatically.

    You have to use the dag_maker as a context manager and it takes
    the same argument as DAG::

        with dag_maker(dag_id="mydag") as dag:
            task1 = EmptyOperator(task_id="mytask")
            task2 = EmptyOperator(task_id="mytask2")

    If the DagModel you want to use needs different parameters than the one
    automatically created by the dag_maker, you have to update the DagModel as below::

        dag_maker.dag_model.is_active = False
        session.merge(dag_maker.dag_model)
        session.commit()

    For any test you use the dag_maker, make sure to create a DagRun::

        dag_maker.create_dagrun()

    The dag_maker.create_dagrun takes the same arguments as dag.create_dagrun
    """
    # IMPORTANT: Delay _all_ imports from `airflow.*` to _inside a method_.
    # This fixture is "called" early on in the pytest collection process, and
    # if we import airflow.* here the wrong (non-test) config will be loaded
    # and "baked" in to various constants

    from airflow.utils.log.logging_mixin import LoggingMixin

    class DagFactory(LoggingMixin):
        _own_session = False

        def __init__(self):
            from airflow.models import DagBag

            # Keep all the serialized dags we've created in this test
            self.dagbag = DagBag(
                os.devnull, include_examples=False, read_dags_from_db=False
            )

        def __enter__(self):
            self.dag.__enter__()
            return self.dag

        def __exit__(self, type, value, traceback):
            from airflow.models import DagModel

            dag = self.dag
            dag.__exit__(type, value, traceback)
            if type is not None:
                return

            dag.clear(session=self.session)
            dag.sync_to_db(processor_subdir=self.processor_subdir, session=self.session)
            self.dag_model = self.session.get(DagModel, dag.dag_id)
            self.dagbag.bag_dag(self.dag, self.dag)

        def create_dagrun(self, **kwargs):
            from airflow.utils import timezone
            from airflow.utils.state import State
            from airflow.utils.types import DagRunType

            dag = self.dag
            kwargs = {
                "state": State.RUNNING,
                "start_date": self.start_date,
                "session": self.session,
                **kwargs,
            }
            # Need to provide run_id if the user does not either provide one
            # explicitly, or pass run_type for inference in dag.create_dagrun().
            if "run_id" not in kwargs and "run_type" not in kwargs:
                kwargs["run_id"] = "test"

            if "run_type" not in kwargs:
                kwargs["run_type"] = DagRunType.from_run_id(kwargs["run_id"])
            if kwargs.get("execution_date") is None:
                if kwargs["run_type"] == DagRunType.MANUAL:
                    kwargs["execution_date"] = self.start_date
                else:
                    kwargs["execution_date"] = dag.next_dagrun_info(None).logical_date
            if "data_interval" not in kwargs:
                logical_date = timezone.coerce_datetime(kwargs["execution_date"])
                if kwargs["run_type"] == DagRunType.MANUAL:
                    data_interval = dag.timetable.infer_manual_data_interval(
                        run_after=logical_date
                    )
                else:
                    data_interval = dag.infer_automated_data_interval(logical_date)
                kwargs["data_interval"] = data_interval

            self.dag_run = dag.create_dagrun(**kwargs)
            for ti in self.dag_run.task_instances:
                ti.refresh_from_task(dag.get_task(ti.task_id))
            return self.dag_run

        def __call__(
            self,
            dag_id="test_dag",
            fileloc=None,
            processor_subdir=None,
            session=None,
            **kwargs,
        ):
            from airflow import settings
            from airflow.models.dag import DAG
            from airflow.utils import timezone

            if session is None:
                self._own_session = True
                session = settings.Session()

            self.kwargs = kwargs
            self.session = session
            self.start_date = self.kwargs.get("start_date", None)
            default_args = kwargs.get("default_args", None)
            if default_args and not self.start_date:
                if "start_date" in default_args:
                    self.start_date = default_args.get("start_date")
            if not self.start_date:
                if hasattr(request.module, "DEFAULT_DATE"):
                    self.start_date = getattr(request.module, "DEFAULT_DATE")
                else:
                    DEFAULT_DATE = timezone.datetime(2016, 1, 1)
                    self.start_date = DEFAULT_DATE
            self.kwargs["start_date"] = self.start_date
            self.dag = DAG(dag_id, **self.kwargs)
            self.dag.fileloc = fileloc or request.module.__file__
            self.processor_subdir = processor_subdir

            return self

        def cleanup(self):
            from airflow.models import DagModel, DagRun, TaskInstance, XCom
            from airflow.models.dataset import DatasetEvent
            from airflow.models.taskmap import TaskMap
            from airflow.utils.retries import run_with_db_retries

            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    dag_ids = list(self.dagbag.dag_ids)
                    if not dag_ids:
                        return
                    # To isolate problems here with problems from elsewhere on the session object
                    self.session.rollback()

                    self.session.query(DagRun).filter(
                        DagRun.dag_id.in_(dag_ids)
                    ).delete(
                        synchronize_session=False,
                    )
                    self.session.query(TaskInstance).filter(
                        TaskInstance.dag_id.in_(dag_ids)
                    ).delete(
                        synchronize_session=False,
                    )
                    self.session.query(XCom).filter(XCom.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(DagModel).filter(
                        DagModel.dag_id.in_(dag_ids)
                    ).delete(
                        synchronize_session=False,
                    )
                    self.session.query(TaskMap).filter(
                        TaskMap.dag_id.in_(dag_ids)
                    ).delete(
                        synchronize_session=False,
                    )
                    self.session.query(DatasetEvent).filter(
                        DatasetEvent.source_dag_id.in_(dag_ids)
                    ).delete(
                        synchronize_session=False,
                    )
                    self.session.commit()
                    if self._own_session:
                        self.session.expunge_all()

    factory = DagFactory()

    try:
        yield factory
    finally:
        factory.cleanup()
        with suppress(AttributeError):
            del factory.session


@pytest.fixture()
def create_task_instance_of_operator(dag_maker):
    def _create_task_instance(
        operator_class,
        *,
        dag_id,
        session=None,
        **operator_kwargs,
    ) -> TaskInstance:
        with dag_maker(dag_id=dag_id, session=session):
            operator_class(**operator_kwargs)
        (ti,) = dag_maker.create_dagrun({}).task_instances
        return ti

    return _create_task_instance


class BasePythonTest:
    opcls: type[BaseOperator]
    dag_id: str
    task_id: str
    run_id: str
    dag: DAG
    ds_templated: str
    default_date: datetime = DEFAULT_DATE

    @pytest.fixture(autouse=True)
    def base_tests_setup(self, request, dag_maker):
        self.dag_id = f"dag_{slugify(request.cls.__name__)}"
        self.dag_maker = dag_maker
        self.dag = self.dag_maker(self.dag_id).dag
        clear_db_runs()
        yield
        clear_db_runs()

    def create_dag_run(self) -> DagRun:
        return self.dag.create_dagrun(
            state=DagRunState.RUNNING,
            start_date=self.dag_maker.start_date,
            session=self.dag_maker.session,
            execution_date=self.default_date,
            run_type=DagRunType.MANUAL,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        )
