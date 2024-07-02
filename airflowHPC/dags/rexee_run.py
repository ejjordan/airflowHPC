from airflow import DAG
from airflow.utils import timezone
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflowHPC.dags.replex import read_counter
from airflowHPC.dags.tasks import run_if_false, get_file, evaluate_template_truth

with DAG(
    "REXEE_runner",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "num_iterations": 3,
        "num_simulations": 4,
        "output_dir": "outputs",
        "dhdl_store_dir": "dhdl",
        "dhdl_store_fn": "dhdl.json",
    },
) as dag:
    dag.doc = """This is a DAG for running iterations of a REXEE simulation."""

    initialize_rexee_params = {
        "num_simulations": "{{ params.num_simulations }}",
        "output_dir": "{{ params.output_dir }}",
        "dhdl_store_dir": "{{ params.dhdl_store_dir }}",
        "dhdl_store_fn": "{{ params.dhdl_store_fn }}",
    }
    is_initialize_done = get_file.override(task_id="is_initialize_done")(
        input_dir="{{ params.output_dir }}/{{ params.dhdl_store_dir }}",
        file_name="{{ params.dhdl_store_fn }}",
        use_ref_data=False,
        check_exists=True,
    )
    initialize_rexee = run_if_false.override(group_id="initialize")(
        "REXEE_initialization", initialize_rexee_params, is_initialize_done
    )
    # Here we can be sure that there is a counter file
    last_iteration_num = read_counter.override(
        task_id="read_counter", trigger_rule="none_failed"
    )("{{ params.output_dir }}")
    continue_rexee_params = {
        "last_iteration_num": last_iteration_num,
        "num_iterations": "{{ params.num_iterations }}",
        "num_simulations": "{{ params.num_simulations }}",
        "output_dir": "{{ params.output_dir }}",
        "dhdl_store_dir": "{{ params.dhdl_store_dir }}",
        "dhdl_store_fn": "{{ params.dhdl_store_fn }}",
    }
    is_continue_done = evaluate_template_truth.override(
        task_id="is_continue_done", trigger_rule="none_failed"
    )(
        statement="{{ params.num_iterations }} <= {{ task_instance.xcom_pull(task_ids='read_counter') }}",
    )
    continue_rexee = run_if_false.override(group_id="continue")(
        "REXEE_continuation", continue_rexee_params, is_continue_done
    )
    initialize_rexee >> last_iteration_num >> is_continue_done >> continue_rexee
