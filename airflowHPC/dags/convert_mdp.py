from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file
from airflowHPC.utils.mdp2json import validate_convert_mdp


with DAG(
    dag_id="convert_mdp",
    start_date=timezone.utcnow(),
    catchup=False,
    params={
        "mdp": {"directory": "anthracene", "filename": "dg.mdp"},
        "output_dir": "output",
        "output_filename": "mdout.json",
    },
) as dag:
    input_mdp = get_file.override(task_id="get_mdp_input")(
        input_dir="{{ params.mdp.directory }}",
        file_name="{{ params.mdp.filename }}",
        use_ref_data=True,
    )
    mdp_json_dict = validate_convert_mdp.override(task_id="validate_convert_mdp")(
        mdp_file_path=input_mdp,
        output_file_path="{{ params.output_dir }}/{{ params.output_filename }}",
    )
