from airflow import DAG
from airflow.utils import timezone
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflowHPC.dags.tasks import get_file, branch_task

with DAG(
    dag_id="fs_peptide",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"num_simulations": 4},
) as fs_peptide:
    fs_peptide.doc = """Replica exchange simulation of a peptide in water."""

    is_setup_done = get_file.override(task_id="is_setup_done")(
        input_dir="prep",
        file_name="system_solv.gro",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_setup = TriggerDagRunOperator(
        task_id="trigger_setup",
        trigger_dag_id="prepare_system",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
    )
    setup_done = EmptyOperator(task_id="setup_done", trigger_rule="none_failed")
    setup_done_branch = branch_task.override(task_id="setup_done_branch")(
        truth_value=is_setup_done,
        task_if_true=setup_done.task_id,
        task_if_false=trigger_setup.task_id,
    )

    is_setup_done >> setup_done_branch >> [trigger_setup, setup_done]
