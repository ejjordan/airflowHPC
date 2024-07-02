from airflow import DAG
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    list_from_xcom,
)
from airflowHPC.dags.replex import (
    prepare_args_for_mdp_functions,
    initialize_MDP,
    run_iteration,
    reduce_dhdl,
    store_dhdl_results,
    increment_counter,
)

with DAG(
    "REXEE_initialization",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        # "iteration": 1,
        "num_simulations": 4,
        "output_dir": "outputs",
        "dhdl_store_dir": "dhdl",
        "dhdl_store_fn": "dhdl.json",
    },
) as dag:
    dag.doc = """Demonstration of a REXEE workflow.
    To rerun the DAG, outputs from any previous instance must be deleted first."""

    counter = increment_counter("{{ params.output_dir }}")
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="ensemble_md", file_name="expanded.mdp"
    )
    expand_args = prepare_args_for_mdp_functions(counter, mode="initialize")
    mdp_inputs = (
        initialize_MDP.override(task_id="intialize_mdp")
        .partial(template_mdp=input_mdp)
        .expand(expand_args=expand_args)
    )
    mdp_inputs_list = list_from_xcom.override(task_id="get_mdp_input_list")(mdp_inputs)
    grompp_input_list = prepare_gmxapi_input(
        args=["grompp"],
        input_files={"-f": mdp_inputs_list, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr", "-po": "mdout.mdp"},
        output_dir="{{ params.output_dir }}",
        counter=counter,
        num_simulations="{{ params.num_simulations }}",
    )
    dhdl_results = run_iteration(grompp_input_list)
    dhdl_dict = reduce_dhdl(
        dhdl_results, counter
    )  # key: iteration number; value: a list of dictionaries with keys like simulation_id, state, and gro
    dhdl_store = store_dhdl_results(
        dhdl_dict=dhdl_dict,
        output_dir="{{ params.output_dir }}/{{ params.dhdl_store_dir }}",
        output_fn="{{ params.dhdl_store_fn }}",
        iteration=counter,
    )
