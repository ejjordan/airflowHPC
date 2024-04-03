from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import (
    get_file,
    run_gmxapi,
    prepare_gmxapi_input,
    update_gmxapi_input,
)
from airflowHPC.operators.radical_operator import RadicalOperator

with DAG(
    "run_gmxapi",
    start_date=timezone.utcnow(),
    catchup=False,
    params={"output_dir": "outputs"},
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
        num_simulations=1,
    )
    grompp_result = RadicalOperator(
        task_id="grompp", queue="radical", input_data=grompp_input
    )
    mdrun_input = update_gmxapi_input.override(task_id="mdrun_prepare")(
        args=["mdrun"],
        input_files_keys={"-s": "-o"},
        output_files={
            "-c": "result.gro",
            "-x": "result.xtc",
        },
        gmxapi_output=grompp_result.output,
    )
    mdrun_result = RadicalOperator(
        task_id="mdrun", queue="radical", input_data=mdrun_input
    )

    grompp_input >> grompp_result >> mdrun_input >> mdrun_result
    """
    grompp_result = run_gmxapi.override(task_id="grompp")(
        args=["grompp"],
        input_files={"-f": input_mdp, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_result = run_gmxapi.override(task_id="mdrun")(
        args=["mdrun"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
    """
