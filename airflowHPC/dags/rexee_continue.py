from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    list_from_xcom,
    evaluate_template_truth,
    run_if_false,
)
from airflowHPC.dags.replex import (
    increment_counter,
    prepare_args_for_mdp_functions,
    run_iteration,
    reduce_dhdl,
    store_dhdl_results,
    get_swaps,
    update_MDP,
    prepare_next_step,
    get_dhdl,
    extract_final_dhdl_info,
)


@task()
def path_as_dataset(dataset_path: str) -> Dataset:
    return Dataset(uri=dataset_path)


with DAG(
    "REXEE_continuation",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "last_iteration_num": 2,
        "num_iterations": 3,
        "num_simulations": 4,
        "num_steps": 2000,
        "num_states": 9,
        "shift_range": 1,
        "temperature": 298,
        "output_dir": "outputs",
        "dhdl_store_dir": "dhdl",
        "dhdl_store_fn": "dhdl.json",
    },
) as dag:
    dag.doc = """This is a DAG for running iterations of a REXEE simulation."""

    dhdl_store = path_as_dataset(
        dataset_path="{{ params.output_dir }}/{{ params.dhdl_store_dir }}/{{ params.dhdl_store_fn }}"
    )
    swap_pattern = get_swaps(
        iteration="{{ params.last_iteration_num}}",  # Swaps are determined based on the previous iteration
        num_simulations="{{ params.num_simulations }}",
        num_states="{{ params.num_states }}",
        shift_range="{{ params.shift_range }}",
        temperature="{{ params.temperature }}",
        dhdl_store=dhdl_store,
    )  # Figure out the swapping pattern based on the previous iteration

    # Update MDP files for the next iteration (Note that here we update the counter first.)
    expand_args = prepare_args_for_mdp_functions(
        counter="{{ params.last_iteration_num}}",
        mode="update",
        num_simulations="{{ params.num_simulations }}",
    )
    mdp_updates = (
        update_MDP.override(task_id="update_mdp")
        .partial(
            iter_idx="{{ params.last_iteration_num}}",
            dhdl_store=dhdl_store,
            num_simulations="{{ params.num_simulations }}",
            num_steps="{{ params.num_steps }}",
            shift_range="{{ params.shift_range }}",
        )
        .expand(expand_args=expand_args)
    )
    mdp_updates_list = list_from_xcom.override(task_id="get_mdp_update_list")(
        values=mdp_updates
    )

    # Run the next iteration
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    next_step_input = prepare_next_step(
        top_path=input_top,
        mdp_path=mdp_updates_list,
        swap_pattern=swap_pattern,
        dhdl_store=dhdl_store,
        iteration="{{ params.last_iteration_num}}",
    )

    this_iteration_num = increment_counter(output_dir="outputs")
    mdrun_result = run_iteration(grompp_input_list=next_step_input)
    dhdl = mdrun_result.map(get_dhdl)
    dhdl_results = extract_final_dhdl_info.partial(
        shift_range="{{ params.shift_range }}"
    ).expand(result=dhdl)
    next_step_input >> this_iteration_num >> mdrun_result >> dhdl_results
    dhdl_dict = reduce_dhdl(dhdl=dhdl_results, iteration=this_iteration_num)
    dhdl_store = store_dhdl_results(
        dhdl_dict=dhdl_dict,
        output_dir="{{ params.output_dir }}/{{ params.dhdl_store_dir }}",
        output_fn="{{ params.dhdl_store_fn }}",
        iteration=this_iteration_num,
    )

    is_continue_done = evaluate_template_truth.override(
        task_id="is_continue_done", trigger_rule="none_failed"
    )(
        statement="{{ params.num_iterations }} <= {{ task_instance.xcom_pull(task_ids='increment_counter') }}",
    )
    continue_rexee_params = {
        "last_iteration_num": this_iteration_num,
        "num_iterations": "{{ params.num_iterations }}",
        "num_simulations": "{{ params.num_simulations }}",
        "output_dir": "{{ params.output_dir }}",
        "dhdl_store_dir": "{{ params.dhdl_store_dir }}",
        "dhdl_store_fn": "{{ params.dhdl_store_fn }}",
    }

    continue_rexee = run_if_false.override(group_id="continue")(
        dag_id="REXEE_continuation",
        dag_params=continue_rexee_params,
        truth_value=is_continue_done,
        wait_for_completion=False,
    )
    dhdl_store >> is_continue_done >> continue_rexee
