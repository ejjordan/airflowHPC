from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow import Dataset

from airflowHPC.dags.tasks import InputHolder, prepare_input


SHIFT_RANGE = 1
NUM_ITERATIONS = 2
NUM_SIMULATIONS = 4
STATE_RANGES = [
    [0, 1, 2, 3, 4, 5],
    [1, 2, 3, 4, 5, 6],
    [2, 3, 4, 5, 6, 7],
    [3, 4, 5, 6, 7, 8],
]


@task.branch
def check_condition(counter, num_iterations):
    import logging

    logging.info(f"check_condition: counter {counter} iterations {num_iterations}")
    if counter < num_iterations:
        return "trigger_self"
    else:
        return "run_complete"


@task
def run_complete():
    import logging

    logging.info("run_complete: done")
    return "done"


def get_dhdl(result):
    return {
        "simulation_id": result["simulation_id"],
        "-dhdl": result["-dhdl"],
        "gro_path": result["-c"],
    }


@task
def extract_final_dhdl_info(result) -> dict[str, int]:
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    shift_range = SHIFT_RANGE
    i: int = result["simulation_id"]
    dhdl = result["-dhdl"]
    gro = result["gro_path"]
    headers = get_headers(dhdl)
    state_local = list(extract_dataframe(dhdl, headers=headers)["Thermodynamic state"])[
        -1
    ]  # local index of the last state  # noqa: E501
    state_global: int = state_local + i * shift_range  # global index of the last state
    return {"simulation_id": i, "state": state_global, "gro": gro}


@task
def reduce_dhdl(dhdl, iteration):
    return {str(iteration): list(dhdl)}


@task
def store_dhdl_results(dhdl_dict, output_dir, iteration) -> Dataset:
    import os
    import json
    import logging

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, "dhdl.json")
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            data = json.load(f)
        if str(iteration) in data["iteration"].keys():
            raise ValueError(
                f"store_dhdl_results: iteration {iteration} already exists in {output_file}"
            )
        if str(iteration) not in dhdl_dict.keys():
            raise ValueError(
                f"store_dhdl_results: iteration {iteration} not found in dhdl_dict"
            )
        data["iteration"][str(iteration)] = dhdl_dict[str(iteration)]
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2, separators=(",", ": "))
    else:
        if str(iteration) not in dhdl_dict.keys():
            raise ValueError(
                f"store_dhdl_results: iteration {iteration} not found in dhdl_dict"
            )
        with open(output_file, "w") as f:
            data = {"iteration": {str(iteration): dhdl_dict[str(iteration)]}}
            json.dump(data, f, indent=2, separators=(",", ": "))
    dataset = Dataset(uri=output_file)
    logging.info(f"store_dhdl_results: iteration {iteration} dataset {dataset}")
    return dataset


@task
def get_swaps(
    iteration, dhdl_store, state_ranges=STATE_RANGES, neighbor_exchange=False
):
    from itertools import combinations
    import numpy as np
    import logging
    import random
    import json

    with open(dhdl_store.uri, "r") as f:
        data = json.load(f)

    states = [
        data["iteration"][str(iteration)][i]["state"]
        for i in range(len(data["iteration"][str(iteration)]))
    ]
    sim_idx = [
        data["iteration"][str(iteration)][i]["simulation_id"]
        for i in range(len(data["iteration"][str(iteration)]))
    ]

    all_pairs = list(combinations(sim_idx, 2))

    # First, we identify pairs of replicas with overlapping ranges
    swappables = [
        i
        for i in all_pairs
        if set(state_ranges[i[0]]).intersection(set(state_ranges[i[1]])) != set()
    ]

    # Next, we exclude the ones where the last sampled states are not present in both alchemical ranges
    swappables = [
        i
        for i in swappables
        if states[i[0]] in state_ranges[i[1]] and states[i[1]] in state_ranges[i[0]]
    ]

    if neighbor_exchange is True:
        swappables = [i for i in swappables if np.abs(i[0] - i[1]) == 1]

    logging.info(f"get_swaps: iteration {iteration} swappables {swappables}")

    swap_pattern = sim_idx  # initialize with no swaps
    if len(swappables) > 0:
        swap = random.choices(swappables, k=1)[0]
        swap_pattern[swap[0]], swap_pattern[swap[1]] = (
            swap_pattern[swap[1]],
            swap_pattern[swap[0]],
        )
        state_ranges[swap[0]], state_ranges[swap[1]] = (
            state_ranges[swap[1]],
            state_ranges[swap[0]],
        )
    logging.info(f"get_swaps: iteration {iteration} swap_pattern {swap_pattern}")

    return swap_pattern


@task
def prepare_next_step(swap_pattern, inputHolderList, dhdl_dict, iteration):
    from dataclasses import asdict

    input_holder_list = [InputHolder(**i) for i in inputHolderList]
    if str(iteration) not in dhdl_dict.keys():
        raise ValueError(
            f"prepare_next_step: iteration {iteration} not found in dhdl_dict"
        )
    dhdl_info = dhdl_dict[str(iteration)]

    # Swap the gro files but keep the order for top and mdp files
    gro_list = [
        dhdl_info[i]["gro"] for i in swap_pattern if dhdl_info[i]["simulation_id"] == i
    ]
    next_step_input = [
        asdict(
            InputHolder(
                gro_path=gro_list[i],
                top_path=input_holder_list[i].top_path,
                mdp_path=input_holder_list[i].mdp_path,
                simulation_id=i,
                output_dir=f"outputs/step_{iteration}/sim_{i}",
            )
        )
        for i in range(len(swap_pattern))
    ]
    return next_step_input


@task_group
def run_iteration(inputs_list):
    from airflowHPC.dags.tasks import run_grompp, run_mdrun

    grompp_result = run_grompp.expand(input_holder_dict=inputs_list)
    mdrun_result = run_mdrun.expand(mdrun_holder_dict=grompp_result)
    dhdl = mdrun_result.map(get_dhdl)
    dhdl_result = extract_final_dhdl_info.expand(result=dhdl)
    return dhdl_result


@task(max_active_tis_per_dag=1)
def increment_counter(output_dir):
    import os

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    counter_file = os.path.join(out_path, "counter.txt")
    # start at index 1 so that requesting n iterations will run n iterations
    counter = 1
    if os.path.exists(counter_file):
        with open(counter_file, "r") as f:
            counter = int(f.read())
        with open(counter_file, "w") as f:
            f.write(str(counter + 1))
    else:
        with open(counter_file, "w") as f:
            f.write(str(counter + 1))
    return counter


with DAG(
    "replica_exchange",
    start_date=timezone.utcnow(),
    schedule="@once",
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    tags=["schedule"],
) as dag:
    dag.doc = """Demonstration of a replica exchange MD workflow.
    Since it is scheduled '@once', it has to be deleted from the database before it can be run again."""

    counter = increment_counter("outputs")
    inputHolderList = prepare_input(counter=counter, num_simulations=NUM_SIMULATIONS)
    dhdl_results = run_iteration(inputHolderList)
    dhdl_dict = reduce_dhdl(dhdl_results, counter)
    dhdl_store = store_dhdl_results(
        output_dir="outputs/dhdl", iteration=counter, dhdl_dict=dhdl_dict
    )
    swap_pattern = get_swaps(iteration=counter, dhdl_store=dhdl_store)
    next_step_input = prepare_next_step(
        swap_pattern, inputHolderList, dhdl_dict, counter
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_self", trigger_dag_id="replica_exchange"
    )
    condition = check_condition(counter, NUM_ITERATIONS)
    done = run_complete()
    condition.set_upstream(next_step_input)
    trigger.set_upstream(condition)
    done.set_upstream(condition)
