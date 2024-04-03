from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
    _run_gmxapi,
    GmxapiInputHolder,
    GmxapiRunInfoHolder,
)
from airflowHPC.operators.radical_operator import RadicalOperator


with DAG(
    "run_gmxapi_multi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={"output_dir": "multi", "expected_output": "result.gro"},
) as dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="ensemble_md", file_name="expanded.mdp"
    )
    grompp_input = prepare_gmxapi_input.override(task_id="grompp_prepare")(
        args=["grompp"],
        input_files={
            "-f": input_mdp,
            "-c": input_gro,
            "-p": input_top,
        },
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
        counter=0,
        num_simulations=2,
    )
    grompp_result = RadicalOperator.partial(task_id="grompp", queue="radical").expand(
        input_data=grompp_input,
    )

    """
    grompp_result = run_gmxapi_dataclass.override(task_id="grompp").expand(
        input_data=grompp_input
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-c": "{{ params.expected_output }}",
                "-x": "{{ params.expected_output | replace('gro', 'xtc') }}",
            },
        )
        .expand(gmxapi_output=grompp_result)
    )
    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun").expand(
        input_data=mdrun_input
    )
    """
