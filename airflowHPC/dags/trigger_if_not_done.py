from airflow import DAG
from airflow.utils import timezone
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflowHPC.dags.tasks import get_file, branch_task


with DAG(
    dag_id="trigger_if_not_done",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"input_dir": "prep", "input_file": "nvt.gro", "dag_id": "system_setup", "dag_params": {"input_dir": "ala_tripeptide_remd", "output_dir": "prep", "input_pdb": "ala_tripeptide.pdb",
                "box_size": 3}},
) as triggerer:
    triggerer.doc = """Triggers a DAG conditionally based on the existence of a file."""

    is_dag_done = get_file.override(task_id=f"is_{triggerer.params.get_param('dag_id').value}_done")(
        input_dir="{{ params.input_dir }}",
        file_name="{{ params.input_file }}",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_dag = TriggerDagRunOperator(
        task_id=f"trigger_{triggerer.params.get_param('dag_id').value}",
        trigger_dag_id="{{ params.dag_id }}",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        params=triggerer.params.get_param("dag_params").value
    )
    dag_done = EmptyOperator(task_id=f"{triggerer.params.get_param('dag_id').value}_done", trigger_rule="none_failed")
    dag_done_branch = branch_task.override(task_id=f"{triggerer.params.get_param('dag_id').value}_done_branch")(
        truth_value=is_dag_done,
        task_if_true=dag_done.task_id,
        task_if_false=trigger_dag.task_id,
    )
    is_dag_done >> dag_done_branch >> [trigger_dag, dag_done]