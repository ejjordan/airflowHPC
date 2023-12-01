from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow import Dataset

from tasks import run_grompp


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


@task(multiple_outputs=True, max_active_tis_per_dag=1)
def run_mdrun(tpr_path: str, output_info) -> dict:
    import os
    import gmxapi as gmx

    output_dir = output_info[0]
    simulation_id = output_info[1]
    if not os.path.exists(tpr_path):
        raise FileNotFoundError("You must supply a tpr file")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    input_files = {"-s": tpr_path}
    output_files = {
        "-x": os.path.join(out_path, "result.xtc"),
        "-c": os.path.join(out_path, "result.gro"),
        "-dhdl": os.path.join(out_path, "dhdl.xvg"),
    }
    cwd = os.getcwd()
    os.chdir(out_path)
    md = gmx.commandline_operation(
        gmx.commandline.cli_executable(), "mdrun", input_files, output_files
    )
    md.run()
    os.chdir(cwd)
    assert os.path.exists(md.output.file["-c"].result())
    results_dict = md.output.file.result()
    results_dict["simulation_id"] = simulation_id
    return results_dict


def get_dhdl(result):
    return {"simulation_id": result["simulation_id"], "-dhdl": result["-dhdl"]}


def get_state(result):
    return {"simulation_id": result["simulation_id"], "state": result["state"]}


@task
def extract_final_dhdl_info(result) -> dict[str, int]:
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    shift_range = SHIFT_RANGE
    i = result["simulation_id"]
    dhdl = result["-dhdl"]
    headers = get_headers(dhdl)
    state_local = list(extract_dataframe(dhdl, headers=headers)["Thermodynamic state"])[
        -1
    ]  # local index of the last state  # noqa: E501
    state_global = state_local + i * shift_range  # global index of the last state
    return {"simulation_id": i, "state": state_global}


@task
def store_dhdl_results(dhdl, output_dir, iteration) -> Dataset:
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
            data["iteration"][str(iteration)].append(dhdl)
        else:
            data["iteration"][str(iteration)] = [dhdl]
        with open(output_file, "w") as f:
            json.dump(data, f, indent=4, separators=(",", ": "))
    else:
        with open(output_file, "w") as f:
            data = {"iteration": {str(iteration): [dhdl]}}
            json.dump(data, f, indent=4, separators=(",", ": "))
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

    # Only one dataset is actually used so it should be fine to just take the first one
    store = list(dhdl_store)[0]
    with open(store.uri, "r") as f:
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


@task_group
def run_iteration(tpr_ref, num_replicates, iteration):
    mdrun_outputs_info = [(f"outputs/sim_{i}", i) for i in range(num_replicates)]
    mdrun_result = run_mdrun.partial(tpr_path=tpr_ref["-o"]).expand(
        output_info=mdrun_outputs_info
    )
    dhdl = mdrun_result.map(get_dhdl)
    dhdl_result = extract_final_dhdl_info.expand(result=dhdl)
    state = dhdl_result.map(get_state)
    dhdl_store = store_dhdl_results.partial(
        output_dir="outputs/dhdl", iteration=iteration
    ).expand(dhdl=state)
    swap_pattern = get_swaps(iteration=iteration, dhdl_store=dhdl_store)
    return swap_pattern


@task(max_active_tis_per_dag=1)
def increment_counter(output_dir):
    import os

    if not os.path.exists(output_dir):
        raise FileNotFoundError("Output directory does not exist")
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
    "looper",
    schedule=None,
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as dag:
    grompp_result = run_grompp("sys.gro", "outputs/grompp")
    counter = increment_counter("outputs")
    swap_pattern = run_iteration(grompp_result, NUM_SIMULATIONS, counter)
    trigger = TriggerDagRunOperator(task_id="trigger_self", trigger_dag_id="looper")
    condition = check_condition(counter, NUM_ITERATIONS)
    done = run_complete()
    condition.set_upstream(swap_pattern)
    trigger.set_upstream(condition)
    done.set_upstream(condition)
