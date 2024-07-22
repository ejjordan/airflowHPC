from typing import Dict, List
from airflow.decorators import task, task_group
from airflow import Dataset


STATE_RANGES = [
    [0, 1, 2, 3, 4, 5],
    [1, 2, 3, 4, 5, 6],
    [2, 3, 4, 5, 6, 7],
    [3, 4, 5, 6, 7, 8],
]


@task
def initialize_MDP(
    template_mdp,
    expand_args: dict,
    num_steps: int,
    shift_range: int,
    num_states: int,
    num_simulations: int,
):
    import os
    from ensemble_md.utils import gmx_parser

    idx = expand_args["simulation_id"]
    output_dir = expand_args["output_dir"]

    MDP = gmx_parser.MDP(template_mdp)
    MDP["nsteps"] = num_steps

    start_idx = idx * shift_range

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

    n_sub = num_states - shift_range * (num_simulations - 1)
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
def prepare_args_for_mdp_functions(counter: int, mode: str, num_simulations: int):
    if mode == "initialize":
        # For initializing MDP files for the first iteration
        expand_args = [
            {"simulation_id": i, "output_dir": f"outputs/sim_{i}/iteration_1"}
            for i in range(num_simulations)
        ]
    elif mode == "update":
        # For updating MDP files for the next iteration
        expand_args = [
            {
                "simulation_id": i,
                "template": f"outputs/sim_{i}/iteration_{counter}/expanded.mdp",
                "output_dir": f"outputs/sim_{i}/iteration_{counter+1}",
            }
            for i in range(num_simulations)
        ]
    else:
        raise ValueError('Invalid value for the parameter "mode".')

    return expand_args


@task
def update_MDP(
    iter_idx,
    dhdl_store,
    expand_args: dict,
    num_simulations: int,
    num_steps: int,
    shift_range: int,
):
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
        data["iteration"][str(iter_idx)][i]["state"] for i in range(num_simulations)
    ]

    MDP = gmx_parser.MDP(template)
    MDP["tinit"] = num_steps * MDP["dt"] * iter_idx
    MDP["nsteps"] = num_steps
    MDP["init_lambda_state"] = states[sim_idx] - sim_idx * shift_range

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, f"expanded.mdp")
    MDP.write(output_file, skipempty=True)

    return output_file


@task
def extract_final_dhdl_info(result, shift_range: int) -> Dict[str, int]:
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

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
def store_dhdl_results(dhdl_dict, output_dir, output_fn, iteration) -> Dataset:
    import os
    import json
    import logging

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, output_fn)
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
    dataset = Dataset(uri=str(output_file))
    logging.info(f"store_dhdl_results: iteration {iteration} dataset {dataset}")
    return dataset


def propose_swap(swappables):
    import random

    try:
        swap = random.choices(swappables, k=1)[0]
    except IndexError:  # no swappable pairs
        swap = []

    return swap


def calc_prob_acc(
    swap,
    dhdl_files,
    states: List[int],
    shifts: List[int],
    num_states: int,
    shift_range: int,
    num_simulations: int,
    temperature: float,
):
    import logging
    import numpy as np
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    logging.info(f"dhdl_files: {dhdl_files}")

    f0, f1 = dhdl_files[swap[0]], dhdl_files[swap[1]]
    h0, h1 = get_headers(f0), get_headers(f1)
    data_0, data_1 = (
        extract_dataframe(f0, headers=h0).iloc[-1],
        extract_dataframe(f1, headers=h1).iloc[-1],
    )

    n_sub = num_states - shift_range * (num_simulations - 1)
    dhdl_0 = data_0[-n_sub:]
    dhdl_1 = data_1[-n_sub:]

    old_state_0 = states[swap[0]] - shifts[swap[0]]
    old_state_1 = states[swap[1]] - shifts[swap[1]]

    new_state_0 = states[swap[1]] - shifts[swap[0]]
    new_state_1 = states[swap[0]] - shifts[swap[1]]

    logging.info(
        f"old_state_0: {old_state_0}, old_state_1: {old_state_1}, new_state_0: {new_state_0}, new_state_1: {new_state_1}"
    )

    kT = 1.380649e-23 * 6.0221408e23 * temperature / 1000
    dU_0 = (dhdl_0.iloc[new_state_0] - dhdl_0.iloc[old_state_0]) / kT
    dU_1 = (dhdl_1.iloc[new_state_1] - dhdl_1.iloc[old_state_1]) / kT
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
def get_swaps(
    iteration: int,
    dhdl_store,
    num_simulations: int,
    shift_range: int,
    num_states: int,
    temperature: float,
    proposal="exhaustive",
):
    from itertools import combinations
    import numpy as np
    import logging
    import json
    import copy

    logging.info(f"get_swaps: iteration {iteration} store: {dhdl_store}")
    with open(dhdl_store.uri, "r") as f:
        data = json.load(f)

    swap_list = []
    swap_pattern = list(range(num_simulations))
    state_ranges = copy.deepcopy(STATE_RANGES)

    dhdl_files = [
        data["iteration"][str(iteration)][i]["dhdl"] for i in range(num_simulations)
    ]
    states = [
        data["iteration"][str(iteration)][i]["state"] for i in range(num_simulations)
    ]
    sim_idx = list(range(num_simulations))

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

    if proposal == "exhaustive":
        n_ex = int(np.floor(num_simulations / 2))
    elif proposal == "single":
        n_ex = 1
    elif proposal == "neighboring":
        n_ex = 1
        swappables = [i for i in swappables if np.abs(i[0] - i[1]) == 1]
        logging.info(f"get_swaps: Neighboring swappable pairs: {swappables}")
    else:
        raise ValueError(
            f"get_swaps: Invalid value for the parameter 'proposal': {proposal}"
        )
    shifts = list(shift_range * np.arange(num_simulations))
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
            prob_acc = calc_prob_acc(
                swap=swap,
                dhdl_files=dhdl_files,
                states=states,
                shifts=shifts,
                num_states=num_states,
                shift_range=shift_range,
                num_simulations=num_simulations,
                temperature=temperature,
            )
            logging.info(f"get_swaps: Acceptance rate: {prob_acc:.3f}")
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

    logging.info(f"get_swaps: The proposed swap list: {swap_list}")
    logging.info(f"get_swaps: The finally adopted swap pattern: {swap_pattern}")

    return swap_pattern


@task(multiple_outputs=True)
def prepare_next_step(top_path, mdp_path, swap_pattern, dhdl_store, iteration):
    import json
    import logging

    with open(dhdl_store.uri, "r") as f:
        dhdl_dict = json.load(f)
    logging.info(f"prepare_next_step: iteration {iteration} dhdl_dict {dhdl_dict}")
    if str(iteration) not in dhdl_dict["iteration"].keys():
        raise ValueError(
            f"prepare_next_step: iteration {iteration} not found in dhdl_dict"
        )
    dhdl_info = dhdl_dict["iteration"][str(iteration)]

    # Swap the gro files but keep the order for top and mdp files
    gro_list = [
        dhdl_info[i]["gro"] for i in swap_pattern if dhdl_info[i]["simulation_id"] == i
    ]
    next_step_input = {"gro": gro_list, "top": top_path, "mdp": mdp_path}
    return next_step_input


@task(max_active_tis_per_dag=1)
def increment_counter(output_dir):
    import os

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    counter_file = os.path.join(out_path, "counter.txt")
    # start at index 1 so that requesting n iterations will run n iterations
    if os.path.exists(counter_file):
        with open(counter_file, "r") as f:
            counter = int(f.read())
        counter += 1
        with open(counter_file, "w") as f:
            f.write(str(counter))
    else:
        counter = 1
        with open(counter_file, "w") as f:
            f.write(str(counter))
    return counter


@task(max_active_tis_per_dag=1)
def read_counter(input_dir):
    import os

    out_path = os.path.abspath(input_dir)
    counter_file = os.path.join(out_path, "counter.txt")
    if os.path.exists(counter_file):
        with open(counter_file, "r") as f:
            counter = int(f.read())
    else:
        raise ValueError("No counter.txt found!")

    return counter
