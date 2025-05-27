from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.timezone import datetime

from airflowHPC.dags.tasks import (
    get_file,
    list_from_xcom,
    json_from_dataset_path,
)
from airflowHPC.dags.replex import (
    prepare_args_for_mdp_functions,
    initialize_MDP,
    reduce_dhdl,
    store_dhdl_results,
    increment_counter,
    extract_final_dhdl_info,
)

with DAG(
    "REXEE_initialization",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "num_simulations": 4,
        "num_steps": 2000,
        "num_states": 9,
        "shift_range": 1,
        "output_dir": "outputs",
        "dhdl_store_dir": "dhdl",
        "dhdl_store_fn": "dhdl.json",
    },
) as dag:
    dag.doc = """Demonstration of a REXEE workflow.
    To rerun the DAG, outputs from any previous instance must be deleted first."""

    counter = increment_counter(output_dir="{{ params.output_dir }}")
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="ensemble_md", file_name="expanded.mdp"
    )
    expand_args = prepare_args_for_mdp_functions(
        counter=0,
        num_simulations="{{ params.num_simulations }}",
        output_dir="{{ params.output_dir }}",
    )
    mdp_inputs = (
        initialize_MDP.override(task_id="intialize_mdp")
        .partial(
            template_mdp=input_mdp,
            num_simulations="{{ params.num_simulations }}",
            num_steps="{{ params.num_steps }}",
            num_states="{{ params.num_states }}",
            shift_range="{{ params.shift_range }}",
        )
        .expand(expand_args=expand_args)
    )
    mdp_inputs_list = list_from_xcom.override(task_id="get_mdp_input_list")(mdp_inputs)

    rexee_init_grompp_mdrun_params = {
        "inputs": {
            "gro": {
                "task_id": "get_gro",
                "key": None,
            },
            "top": {
                "task_id": "get_top",
                "key": None,
            },
            "mdp": {
                "task_id": "get_mdp_input_list",
                "key": "return_value",
            },
            "parent_dag_id": dag.dag_id,
        },
        "num_simulations": "{{ params.num_simulations }}",
        "output_name": "rexee_init",
        "output_dir": "{{ params.output_dir }}",
        "output_dataset_structure": {
            "simulation_id": "simulation_id",
            "dhdl": "-dhdl",
            "gro_path": "-c",
        },
        "counter": 0,  # start from iteration_0
    }
    rexee_init_dag = TriggerDagRunOperator(
        task_id="rexee_init_dag",
        trigger_dag_id="grompp_mdrun",
        poke_interval=1,
        conf=rexee_init_grompp_mdrun_params,
        wait_for_completion=True,
    )
    input_gro >> rexee_init_dag
    input_top >> rexee_init_dag
    mdp_inputs_list >> rexee_init_dag

    dhdl = json_from_dataset_path.override(task_id="dhdl_results")(
        dataset_path="{{ params.output_dir }}/iteration_0/"
        + f"{rexee_init_grompp_mdrun_params['output_name']}.json",
    )
    rexee_init_dag >> dhdl
    dhdl_results = extract_final_dhdl_info.partial(
        shift_range="{{ params.shift_range }}"
    ).expand(result=dhdl)
    dhdl_dict = reduce_dhdl(
        dhdl=dhdl_results, iteration=counter
    )  # key: iteration number; value: a list of dictionaries with keys like simulation_id, state, and gro
    dhdl_store = store_dhdl_results(
        dhdl_dict=dhdl_dict,
        output_dir="{{ params.output_dir }}/{{ params.dhdl_store_dir }}",
        output_fn="{{ params.dhdl_store_fn }}",
        iteration=counter,
    )
