from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import run_gmxapi


with DAG(
    "ext_tpr_mdrun",
    start_date=timezone.utcnow(),
    params={"output_dir": "ext_tpr_mdrun"},
) as dag:
    mdrun_result = run_gmxapi.override(task_id="mdrun")(
        args=["mdrun"],
        input_files={
            "-s": "{{ task_instance.xcom_pull(task_ids='grompp', dag_id='run_gmxapi', key='-o', include_prior_dates=True) }}"
        },
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_result.template_fields_renderers = {"input_files", "output_dir"}
