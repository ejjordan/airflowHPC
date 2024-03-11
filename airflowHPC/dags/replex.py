from typing import Dict
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow import Dataset

from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    list_from_xcom,
)

SHIFT_RANGE = 1
NUM_ITERATIONS = 2
NUM_SIMULATIONS = 4
NUM_STATES = 8
NUM_STEPS = 2000
STATE_RANGES = [
    [0, 1, 2, 3, 4, 5],
    [1, 2, 3, 4, 5, 6],
    [2, 3, 4, 5, 6, 7],
    [3, 4, 5, 6, 7, 8],
]
T = 300


@task.branch
def check_condition(counter, num_iterations):
    import logging

    logging.info(f"check_condition: counter {counter} iterations {num_iterations}")
    if counter < num_iterations:
        return "trigger_self"
    else:
        return "run_complete"


def get_dhdl(result):
    return {
        "simulation_id": result["inputs"]["simulation_id"],
        "dhdl": result["outputs"]["-dhdl"],
        "gro_path": result["outputs"]["-c"],
    }


@task
def initialize_MDP(template, expand_args):
    import os
    from ensemble_md.utils import gmx_parser

    # idx_output = (idx, output_dir), i.e., a tuple
    idx = expand_args["simulation_id"]
    output_dir = expand_args["output_dir"]

    MDP = gmx_parser.MDP(template)
    MDP["nsteps"] = NUM_STEPS

    start_idx = idx * SHIFT_RANGE

    lambdas_types_all = [
        "fep_lambdas",
        "mass_lambdas",
        "coul_lambdas",
        "vdw_lambdas",
        "bonded_lambdas",
        "restraint_lambdas",
        "temperature_lambdas",
    ]
    lambda_types = []
    for i in lambdas_types_all:
        if i in MDP.keys():
            lambda_types.append(i)

    n_sub = NUM_STATES - SHIFT_RANGE * (NUM_SIMULATIONS - 1)
    for i in lambda_types:
        MDP[i] = MDP[i][start_idx : start_idx + n_sub]

    if "init_lambda_weights" in MDP:
        MDP["init_lambda_weights"] = MDP["init_lambda_weights"][
            start_idx : start_idx + n_sub
        ]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, f"expanded.mdp")
    MDP.write(output_file, skipempty=True)

    return output_file


@task
def prepare_args_for_mdp_functions(counter, mode):
    if mode == "initialize":
        # For initializing MDP files for the first iteration
        expand_args = [
            {"simulation_id": i, "output_dir": f"outputs/sim_{i}/iteration_1"}
            for i in range(NUM_SIMULATIONS)
        ]
    elif mode == "update":
        # For updating MDP files for the next iteration
        expand_args = [
            {
                "simulation_id": i,
                "template": f"outputs/sim_{i}/iteration_{counter}/expanded.mdp",
                "output_dir": f"outputs/sim_{i}/iteration_{counter+1}",
            }
            for i in range(NUM_SIMULATIONS)
        ]
    else:
        raise ValueError('Invalid value for the parameter "mode".')

    return expand_args


@task
def update_MDP(iter_idx, dhdl_store, expand_args):
    # TODO: Parameters for weight-updating REXEE and distance restraints were ignored and should be added when necessary.
    import os
    import json
    from ensemble_md.utils import gmx_parser

    sim_idx = expand_args["simulation_id"]
    template = expand_args["template"]
    output_dir = expand_args["output_dir"]

    with open(dhdl_store.uri, "r") as f:
        data = json.load(f)
    states = [
        data["iteration"][str(iter_idx)][i]["state"] for i in range(NUM_SIMULATIONS)
    ]

    MDP = gmx_parser.MDP(template)
    MDP["tinit"] = NUM_STEPS * MDP["dt"] * iter_idx
    MDP["nsteps"] = NUM_STEPS
    MDP["init_lambda_state"] = states[sim_idx] - sim_idx * SHIFT_RANGE

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, f"expanded.mdp")
    MDP.write(output_file, skipempty=True)

    return output_file


@task
def extract_final_dhdl_info(result) -> Dict[str, int]:
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    shift_range = SHIFT_RANGE
    i: int = result["simulation_id"]
    dhdl = result["dhdl"]
    gro = result["gro_path"]
    headers = get_headers(dhdl)
    state_local = list(extract_dataframe(dhdl, headers=headers)["Thermodynamic state"])[
        -1
    ]  # local index
    state_global: int = state_local + i * shift_range  # global index
    return {"simulation_id": i, "state": state_global, "gro": gro, "dhdl": dhdl}


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


def propose_swap(swappables):
    import random

    try:
        swap = random.choices(swappables, k=1)[0]
    except IndexError:  # no swappable pairs
        swap = []

    return swap


def calc_prob_acc(swap, dhdl_files, states):
    import logging
    import numpy as np
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    logging.info(dhdl_files)
    shifts = [SHIFT_RANGE for i in range(NUM_SIMULATIONS)]
    f0, f1 = dhdl_files[swap[0]], dhdl_files[swap[1]]
    h0, h1 = get_headers(f0), get_headers(f1)
    data_0, data_1 = (
        extract_dataframe(f0, headers=h0).iloc[-1],
        extract_dataframe(f1, headers=h1).iloc[-1],
    )

    n_sub = NUM_STATES - SHIFT_RANGE * (NUM_SIMULATIONS - 1)
    dhdl_0 = data_0[-n_sub:]
    dhdl_1 = data_1[-n_sub:]

    old_state_0 = states[swap[0]] - shifts[swap[0]]
    old_state_1 = states[swap[1]] - shifts[swap[1]]

    new_state_0 = states[swap[1]] - shifts[swap[0]]
    new_state_1 = states[swap[0]] - shifts[swap[1]]

    kT = 1.380649e-23 * 6.0221408e23 * T / 1000
    dU_0 = (dhdl_0[new_state_0] - dhdl_0[old_state_0]) / kT
    dU_1 = (dhdl_1[new_state_1] - dhdl_1[old_state_1]) / kT
    dU = dU_0 + dU_1

    logging.info(
        f"calc_prob_acc: U^i_n - U^i_m = {dU_0:.2f} kT, U^j_m - U^j_n = {dU_1:.2f} kT, Total dU: {dU:.2f} kT"
    )
    prob_acc = min(1, np.exp(-dU))

    return prob_acc


def accept_or_reject(prob_acc):
    import random
    import logging

    if prob_acc == 0:
        swap_bool = False
        logging.info("accept_or_reject: Swap rejected!")
    else:
        rand = random.random()
        logging.info(
            f"accept_or_reject: Acceptance rate: {prob_acc:.3f} / Random number drawn: {rand:.3f}"
        )
        if rand < prob_acc:
            swap_bool = True
            logging.info("accept_or_reject: Swap accepted!")
        else:
            swap_bool = False
            logging.info("accept_or_reject: Swap rejected!")

    return swap_bool


@task
def get_swaps(iteration, dhdl_store):
    from itertools import combinations
    import numpy as np
    import logging
    import random
    import json
    import copy

    with open(dhdl_store.uri, "r") as f:
        data = json.load(f)

    swap_list = []
    swap_pattern = list(range(NUM_SIMULATIONS))
    state_ranges = copy.deepcopy(STATE_RANGES)

    dhdl_files = [
        data["iteration"][str(iteration)][i]["dhdl"] for i in range(NUM_SIMULATIONS)
    ]
    states = [
        data["iteration"][str(iteration)][i]["state"] for i in range(NUM_SIMULATIONS)
    ]
    sim_idx = list(range(NUM_SIMULATIONS))

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

    logging.info(f"get_swaps: iteration {iteration} swappables {swappables}")

    # Note that here we only implement the exhaustive exchange proposal scheme
    n_ex = int(np.floor(NUM_SIMULATIONS / 2))
    shifts = [SHIFT_RANGE for i in range(NUM_SIMULATIONS)]
    for i in range(n_ex):
        if i >= 1:
            swappables = [
                i for i in swappables if set(i).intersection(set(swap)) == set()
            ]
            logging.info(f"get_swaps: Remaining swappable pairs: {swappables}")
        swap = propose_swap(swappables)
        logging.info(f"get_swaps: Proposed swap: {swap}")
        if swap == []:
            logging.info(
                f"get_swaps: No swap is proposed due to the lack of swappable pairs."
            )
            break
        else:
            # Figure out dhdl_files
            prob_acc = calc_prob_acc(swap, dhdl_files, states)
            swap_bool = accept_or_reject(prob_acc)

        if swap_bool is True:
            swap_list.append(swap)
            shifts[swap[0]], shifts[swap[1]] = shifts[swap[1]], shifts[swap[0]]
            dhdl_files[swap[0]], dhdl_files[swap[1]] = (
                dhdl_files[swap[1]],
                dhdl_files[swap[0]],
            )
            swap_pattern[swap[0]], swap_pattern[swap[1]] = (
                swap_pattern[swap[1]],
                swap_pattern[swap[0]],
            )
            state_ranges[swap[0]], state_ranges[swap[1]] = (
                state_ranges[swap[1]],
                state_ranges[swap[0]],
            )
        else:
            pass

    logging.info(f"get_swaps: The finally adopted swap pattern: {swap_pattern}")

    return swap_pattern


@task
def prepare_next_step(top_path, mdp_path, swap_pattern, dhdl_dict, iteration):
    from dataclasses import asdict
    from airflowHPC.dags.tasks import GmxapiInputHolder

    if str(iteration) not in dhdl_dict.keys():
        raise ValueError(
            f"prepare_next_step: iteration {iteration} not found in dhdl_dict"
        )
    dhdl_info = dhdl_dict[str(iteration)]

    # Swap the gro files but keep the order for top and mdp files
    gro_list = [
        dhdl_info[i]["gro"] for i in swap_pattern if dhdl_info[i]["simulation_id"] == i
    ]

    next_step_input = []

    for i in range(len(swap_pattern)):
        if isinstance(top_path, list):
            top = top_path[i]
        else:
            top = top_path
        if isinstance(mdp_path, list):
            mdp = mdp_path[i]
        else:
            mdp = mdp_path

        next_step_input.append(
            asdict(
                GmxapiInputHolder(
                    args=["grompp"],
                    input_files={"-f": mdp, "-c": gro_list[i], "-p": top},
                    output_files={"-o": "run.tpr", "-po": "mdout.mdp"},
                    output_dir=f"outputs/sim_{i}/iteration_{iteration}",
                    simulation_id=i,
                )
            )
        )
    return next_step_input


@task_group
def run_iteration(grompp_input_list):
    from airflowHPC.dags.tasks import run_gmxapi_dataclass, update_gmxapi_input

    grompp_result = run_gmxapi_dataclass.override(task_id="grompp").expand(
        input_data=grompp_input_list
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-dhdl": "dhdl.xvg",
                "-c": "result.gro",
                "-x": "result.xtc",
                "-g": "md.log",
                "-e": "ener.edr",
                "-cpo": "state.cpt",
            },
        )
        .expand(gmxapi_output=grompp_result)
    )

    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun").expand(
        input_data=mdrun_input
    )
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
    "REXEE_example",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as dag:
    dag.doc = """Demonstration of a REXEE workflow.
    Since it is scheduled '@once', it has to be deleted from the database before it can be run again."""

    counter = increment_counter("outputs")
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
        .partial(template=input_mdp)
        .expand(expand_args=expand_args)
    )
    mdp_inputs_list = list_from_xcom.override(task_id="get_mdp_input_list")(mdp_inputs)

    grompp_input_list = prepare_gmxapi_input(
        args=["grompp"],
        input_files={"-f": mdp_inputs_list, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr", "-po": "mdout.mdp"},
        output_dir="outputs",
        counter=counter,
        num_simulations=NUM_SIMULATIONS,
    )
    dhdl_results = run_iteration(grompp_input_list)
    dhdl_dict = reduce_dhdl(
        dhdl_results, counter
    )  # key: iteration number; value: a list of dictionaries with keys like simulation_id, state, and gro
    dhdl_store = store_dhdl_results(
        dhdl_dict=dhdl_dict, output_dir="outputs/dhdl", iteration=counter
    )
    swap_pattern = get_swaps(iteration=counter, dhdl_store=dhdl_store)

    # update MDP files for the next iteration
    expand_args = prepare_args_for_mdp_functions(counter, mode="update")
    mdp_updates = (
        update_MDP.override(task_id="update_mdp")
        .partial(iter_idx=counter, dhdl_store=dhdl_store)
        .expand(expand_args=expand_args)
    )
    mdp_updates_list = list_from_xcom.override(task_id="get_mdp_update_list")(
        mdp_updates
    )

    next_step_input = prepare_next_step(
        input_top, mdp_updates_list, swap_pattern, dhdl_dict, counter
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_self", trigger_dag_id="REXEE_example"
    )
    condition = check_condition(counter, NUM_ITERATIONS)
    run_complete = EmptyOperator(task_id="run_complete", trigger_rule="none_failed")

    next_step_input >> condition >> [trigger, run_complete]
