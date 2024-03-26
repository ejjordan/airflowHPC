import json
import logging
import os

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DagRunNotFound, TaskInstanceNotFound
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskReturnCode
from airflow.utils import cli as cli_utils
from airflow.utils.cli import (
    get_dag,
    get_dag_by_pickle,
)
from airflow.utils.log.file_task_handler import _set_task_deferred_context_var
from airflow.utils.net import get_hostname
from airflow.cli.commands.task_command import (
    RAW_TASK_UNSUPPORTED_OPTION,
    TaskCommandMarker,
    _get_ti,
    _run_task_by_selected_method,
    _move_task_handlers_to_root,
    _redirect_stdout_to_ti_log,
)
from airflow.cli.cli_config import (
    ActionCommand,
    lazy_load_command,
    ARG_DAG_ID,
    ARG_TASK_ID,
    ARG_EXECUTION_DATE_OR_RUN_ID,
    ARG_SUBDIR,
    ARG_MARK_SUCCESS,
    ARG_FORCE,
    ARG_POOL,
    ARG_CFG_PATH,
    ARG_LOCAL,
    ARG_RAW,
    ARG_IGNORE_ALL_DEPENDENCIES,
    ARG_IGNORE_DEPENDENCIES,
    ARG_IGNORE_DEPENDS_ON_PAST,
    ARG_DEPENDS_ON_PAST,
    ARG_SHIP_DAG,
    ARG_PICKLE,
    ARG_JOB_ID,
    ARG_INTERACTIVE,
    ARG_SHUT_DOWN_LOGGING,
    ARG_MAP_INDEX,
    ARG_VERBOSE,
    ARG_READ_FROM_DB,
    TASKS_COMMANDS,
)

log = logging.getLogger(__name__)

rp_task_run = (
    ActionCommand(
        name="run",
        help="Run a single task instance",
        func=lazy_load_command(
            "airflowHPC.cli.commands.radical_command.radical_task_run"
        ),
        args=(
            ARG_DAG_ID,
            ARG_TASK_ID,
            ARG_EXECUTION_DATE_OR_RUN_ID,
            ARG_SUBDIR,
            ARG_MARK_SUCCESS,
            ARG_FORCE,
            ARG_POOL,
            ARG_CFG_PATH,
            ARG_LOCAL,
            ARG_RAW,
            ARG_IGNORE_ALL_DEPENDENCIES,
            ARG_IGNORE_DEPENDENCIES,
            ARG_IGNORE_DEPENDS_ON_PAST,
            ARG_DEPENDS_ON_PAST,
            ARG_SHIP_DAG,
            ARG_PICKLE,
            ARG_JOB_ID,
            ARG_INTERACTIVE,
            ARG_SHUT_DOWN_LOGGING,
            ARG_MAP_INDEX,
            ARG_VERBOSE,
            ARG_READ_FROM_DB,
        ),
    ),
)


@cli_utils.action_cli(check_db=False)
def radical_task_run(args, dag: DAG | None = None) -> TaskReturnCode | None:
    """
    Run a single task instance.

    Note that there must be at least one DagRun for this to start,
    i.e. it must have been scheduled and/or triggered previously.
    Alternatively, if you just need to run it for testing then use
    "airflow tasks test ..." command instead.
    """
    # Load custom airflow config

    if args.local and args.raw:
        raise AirflowException(
            "Option --raw and --local are mutually exclusive. "
            "Please remove one option to execute the command."
        )

    if args.raw:
        unsupported_options = [
            o for o in RAW_TASK_UNSUPPORTED_OPTION if getattr(args, o)
        ]

        if unsupported_options:
            unsupported_raw_task_flags = ", ".join(
                f"--{o}" for o in RAW_TASK_UNSUPPORTED_OPTION
            )
            unsupported_flags = ", ".join(f"--{o}" for o in unsupported_options)
            raise AirflowException(
                "Option --raw does not work with some of the other options on this command. "
                "You can't use --raw option and the following options: "
                f"{unsupported_raw_task_flags}. "
                f"You provided the option {unsupported_flags}. "
                "Delete it to execute the command."
            )
    if dag and args.pickle:
        raise AirflowException(
            "You cannot use the --pickle option when using DAG.cli() method."
        )
    if args.cfg_path:
        with open(args.cfg_path) as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        conf.read_dict(conf_dict, source=args.cfg_path)
        settings.configure_vars()

    settings.MASK_SECRETS_IN_LOGS = True

    get_listener_manager().hook.on_starting(component=TaskCommandMarker())

    if args.pickle:
        print(f"Loading pickle id: {args.pickle}")
        _dag = get_dag_by_pickle(args.pickle)
    elif not dag:
        _dag = get_dag(args.subdir, args.dag_id, args.read_from_db)
    else:
        _dag = dag
    task = _dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(
        task,
        args.map_index,
        exec_date_or_run_id=args.execution_date_or_run_id,
        pool=args.pool,
    )
    ti.init_run_context(raw=args.raw)

    hostname = get_hostname()

    log.info("Running %s on host %s", ti, hostname)

    # IMPORTANT, have to re-configure ORM with the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    # this should be last thing before running, to reduce likelihood of an open session
    # which can cause trouble if running process in a fork.
    settings.reconfigure_orm(disable_connection_pool=True)
    task_return_code = None
    try:
        with _move_task_handlers_to_root(ti), _redirect_stdout_to_ti_log(ti):
            task_return_code = _run_task_by_selected_method(args, _dag, ti)
            if task_return_code == TaskReturnCode.DEFERRED:
                _set_task_deferred_context_var()
    finally:
        try:
            get_listener_manager().hook.before_stopping(component=TaskCommandMarker())
        except Exception:
            pass
    return task_return_code
