from airflow.decorators import task
from airflowHPC.dags.replex import STATE_RANGES, NUM_SIMULATIONS, propose_swap


@task
def get_swaps(iteration, dhdl_store):
    from itertools import combinations
    import logging
    import json
    import copy

    with open(dhdl_store.uri, "r") as f:
        data = json.load(f)

    state_ranges = copy.deepcopy(STATE_RANGES)

    dhdl_files = [
        data["iteration"][str(iteration)][i]["dhdl"] for i in range(NUM_SIMULATIONS)
    ]
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

    logging.info(f"get_swaps: iteration {iteration} swappables {swappables}")

    swap_pattern = sim_idx  # initialize with no swaps
    if len(swappables) > 0:
        swap = propose_swap(swappables)
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
    logging.info(f"get_swaps: iteration {iteration} swap_pattern {swap_pattern}")

    return swap_pattern
