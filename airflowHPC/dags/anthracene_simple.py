from typing import Literal
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    unpack_mdp_options,
    update_gmx_input,
    unpack_param,
    prepare_gmx_input_named,
    dataset_from_xcom_dicts,
    branch_task,
    json_from_dataset_path,
    branch_task_template,
    evaluate_template_truth,
    run_if_false,
)
from airflowHPC.operators import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.models.param import Param

dagrun_params = {
    "inputs": Param(
        {
            "mdp": {"directory": "anthracene", "filename": "dg_mdp.json"},
            "gro": {
                "directory": "anthracene/gro",
                "filename": "lambda_0.00.gro",
            },
            "top": {"directory": "anthracene", "filename": "anthracene.top"},
        },
        type=["object", "null"],
        title="Inputs list",
        items={
            "type": "object",
            "properties": {
                "mdp": {"type": ["object", "null"]},
                "gro": {"type": ["object", "null"]},
                "top": {"type": ["object", "null"]},
            },
            "required": ["mdp", "gro", "top"],
        },
        section="inputs",
    ),
    "mdp_options": [],
    "num_steps": 10000,
    "output_dir": "anthracene_simple",
    "output_name": "anthra",
    "expected_output": "anthra.json",
    "iteration": 1,
    "max_iterations": 3,
    "output_dataset_structure": {
        "dhdl": "-dhdl",
        "gro": "-c",
    },
    "lambda_states_per_step": 5,
    "lambda_states_total": 51,
}


@task
def add_lambdas_to_dataset(dataset_path: str, keys_list: list):
    import json

    with open(dataset_path, "r") as dataset_file:
        dataset = json.load(dataset_file)
    seen_keys = []
    for key in keys_list:
        for i, data in enumerate(dataset):
            if key in data["gro"]:
                dataset[i]["lambda_state"] = key.strip("lambda_")
                seen_keys.append(key)
                break
    assert len(seen_keys) == len(keys_list)
    with open(dataset_path, "w") as dataset_file:
        json.dump(dataset, dataset_file, indent=2, separators=(",", ": "))
    return dataset


@task
def get_lambda_dirs(**context):
    return context["params"]["lambda_dirs"]


with DAG(
    dag_id="anthracene_simple",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params=dagrun_params,
) as anthracene:
    mdp_options = unpack_mdp_options()
    gro_files = unpack_param.override(task_id="get_gro_files")(
        "{{ params.inputs.gro.filename }}"
    )

    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=True,
    )
    gro = (
        get_file.override(task_id="get_gro")
        .partial(input_dir="{{ params.inputs.gro.directory }}", use_ref_data=False)
        .expand(file_name=gro_files)
    )
    mdp_json = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")
        .partial(mdp_json_file_path=mdp_json)
        .expand(update_dict=mdp_options)
    )
    lambda_dirs = get_lambda_dirs()
    grompp_input_list = prepare_gmx_input_named(
        args=["grompp"],
        input_files={
            "-f": mdp,
            "-c": gro,
            "-r": gro,
            "-p": top,
        },
        output_files={"-o": "sim.tpr"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "iteration_{{ params.iteration }}",
        ],
        names=lambda_dirs,
    )
    grompp = ResourceGmxOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list)
    grompp_input_list >> grompp

    mdrun_input_list = (
        update_gmx_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "{{ params.output_name }}.gro", "-dhdl": "dhdl.xvg"},
        )
        .expand(gmx_output=grompp.output)
    )
    mdrun = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 4,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
    dataset = dataset_from_xcom_dicts.override(task_id="make_dataset")(
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        output_fn="{{ params.output_name }}.json",
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun', key='return_value')}}",
        dataset_structure="{{ params.output_dataset_structure }}",
    )
    update_data = add_lambdas_to_dataset.override(task_id="update_data")(
        dataset_path="{{ params.output_dir }}/iteration_{{ params.iteration }}/{{ params.expected_output }}",
        keys_list=lambda_dirs,
    )
    mdrun >> dataset >> update_data


@task
def TI(
    dhdl_data,
    output_dir,
    temp: float = 300.0,
    equil_index: int = 10,
):
    """
    Perform TI analysis on a list of dhdl files.

    returns max and min uncertainty states and their values, unless the min is the first state.
    """
    import logging, os
    from alchemlyb.parsing.gmx import extract_dHdl
    from alchemlyb.preprocessing import subsampling
    from alchemlyb import concat as alchemlyb_concat
    from alchemlyb.estimators import TI
    from alchemlyb.visualisation import plot_ti_dhdl

    dhdl_files = [item[1] for sublist in dhdl_data for item in sublist]
    states = [item[0] for sublist in dhdl_data for item in sublist]
    logging.info(f"Performing TI analysis on {dhdl_files}")
    logging.info(f"States: {states}")
    # Preprocessing
    preprocessed_dhdl_data = []
    for file in dhdl_files:
        dhdl_data = extract_dHdl(file, T=temp)
        dhdl_data_sliced = subsampling.slicing(
            dhdl_data, lower=equil_index, upper=None, step=1, force=True
        )
        decorr_dhdl = subsampling.decorrelate_dhdl(
            dhdl_data_sliced, drop_duplicates=True, sort=True, remove_burnin=False
        )
        preprocessed_dhdl_data.append(decorr_dhdl)

    if not preprocessed_dhdl_data:
        raise ValueError(
            "No dhdl data was processed. Check if .xvg files are read correctly."
        )

    combined_dhdl_data = alchemlyb_concat(preprocessed_dhdl_data)

    # Analysis
    ti = TI().fit(combined_dhdl_data)
    logging.info(
        f"FE differences in units of Kb_T between each lambda window (TI): {ti.delta_f_}"
    )
    logging.info(f"Endpoint differences (TI): {ti.delta_f_.loc[0.0, 1.0]}")
    logging.info(f"TI error: {ti.d_delta_f_}")
    logging.info(f"TI error endpoint difference: {ti.d_delta_f_.loc[0.0, 1.0]}")

    states = sorted(states)
    logging.info(f"lambda values: {states}")
    state_uncertainties = []
    for i, state in enumerate(states):
        if state == "1.00":
            continue
        neighbor_uncertainty = ti.d_delta_f_.loc[float(state), float(states[i + 1])]
        logging.info(
            f"Uncertainty between lambda {state} and lambda {states[i + 1]}: {neighbor_uncertainty}"
        )
        state_uncertainties.append((state, states[i + 1], neighbor_uncertainty))

    # Plotting
    ax = plot_ti_dhdl(ti, labels=["VDW"], colors="r")
    ax.figure.savefig(os.path.join(output_dir, "dhdl_TI.png"))
    return {"uncertainties": state_uncertainties}


@task
def MBAR(
    dhdl_data,
    output_dir,
    temp: float = 300.0,
    equil_index: int = 10,
):
    """
    Perform MBAR analysis on a list of dhdl files.

    returns max and min uncertainty states and their values, unless the min is the first state.
    """
    import logging, os
    from alchemlyb.parsing.gmx import extract_u_nk
    from alchemlyb.preprocessing import subsampling
    from alchemlyb import concat as alchemlyb_concat
    from alchemlyb.estimators import MBAR
    from alchemlyb.visualisation import plot_mbar_overlap_matrix

    dhdl_files = [item[1] for sublist in dhdl_data for item in sublist]
    states = [item[0] for sublist in dhdl_data for item in sublist]
    logging.info(f"Performing MBAR analysis on files {dhdl_files}")
    logging.info(f"States: {states}")
    # Preprocessing
    preprocessed_u_nk_data = []
    for file in dhdl_files:
        u_nk_data = extract_u_nk(file, T=temp)
        u_nk_data_sliced = subsampling.slicing(
            u_nk_data, lower=equil_index, upper=None, step=1, force=True
        )
        decorr_u_nk = subsampling.decorrelate_u_nk(
            u_nk_data_sliced,
            method="all",
            drop_duplicates=True,
            sort=True,
            remove_burnin=False,
        )
        preprocessed_u_nk_data.append(decorr_u_nk)

    if not preprocessed_u_nk_data:
        raise ValueError(
            "No u_nk data was processed. Check if .xvg files are read correctly."
        )

    combined_u_nk_data = alchemlyb_concat(preprocessed_u_nk_data)

    # Analysis
    mbar = MBAR(initial_f_k=None).fit(combined_u_nk_data)
    logging.info(
        f"FE differences in units of Kb_T between each lambda window (MBAR): {mbar.delta_f_}"
    )
    logging.info(f"Endpoint differences (MBAR): {mbar.delta_f_.loc[0.0, 1.0]}")
    logging.info(f"MBAR error: {mbar.d_delta_f_}")
    logging.info(f"MBAR error endpoint difference: {mbar.d_delta_f_.loc[0.0, 1.0]}")

    states = sorted(states)
    logging.info(f"lambda values: {states}")
    state_uncertainties = []
    for i, state in enumerate(states):
        if state == "1.00":
            continue
        neighbor_uncertainty = mbar.d_delta_f_.loc[float(state), float(states[i + 1])]
        logging.info(
            f"Uncertainty between lambda {state} and lambda {states[i+1]}: {neighbor_uncertainty}"
        )
        state_uncertainties.append((state, states[i + 1], neighbor_uncertainty))

    # Plotting
    ax = plot_mbar_overlap_matrix(mbar.overlap_matrix)
    ax.figure.savefig(
        os.path.join(output_dir, "O_MBAR.png"), bbox_inches="tight", pad_inches=0.0
    )
    return {"uncertainties": state_uncertainties}


@task(multiple_outputs=True)
def generate_lambda_states(num_states: int | str):
    """
    Generates a dictionary of lambda states for a given number of states.
    The keys are the state index and the values are the lambda value.
    The lambda values are uniformly spaced between 0 and 1, rounded to 2 decimal places.
    """
    idx_to_state = {
        str(i): f"{round(i / (num_states - 1), 2):.2f}" for i in range(num_states)
    }
    state_to_idx = {v: k for k, v in idx_to_state.items()}
    states = list(idx_to_state.values())
    indices = list(idx_to_state.keys())
    return {
        "idx_to_state": idx_to_state,
        "state_to_idx": state_to_idx,
        "states": states,
        "indices": indices,
    }


@task
def get_new_state(
    results,
    gro_states_dict,
    lambda_states,
    states_per_step,
    method: Literal["TI", "MBAR"],
):
    import logging
    import numpy as np

    gro_states = [gro for gro in gro_states_dict]
    initial_states = [item for sublist in gro_states for item in sublist]
    logging.info(f"initial states: {initial_states}")
    max_states_in_range = int(len(initial_states) / 2)
    uncertainties = results["uncertainties"]
    for result in uncertainties:
        logging.info(
            f"uncertainty between states {result[0]} and {result[1]}: {result[2]}"
        )

    new_state_ranges = {}
    for _ in range(states_per_step):
        max_uncertainty = max(uncertainties, key=lambda x: x[2])
        max_uncertainty_idx = uncertainties.index(max_uncertainty)
        logging.debug(f"max uncertainty: {max_uncertainty}")

        key = (max_uncertainty[0], max_uncertainty[1])
        if key in new_state_ranges:
            new_state_ranges[key] += 1
        else:
            new_state_ranges[key] = 1
        if new_state_ranges[key] >= max_states_in_range:
            uncertainties.pop(max_uncertainty_idx)
        else:
            uncertainties[max_uncertainty_idx][2] = max_uncertainty[2] / 2
        logging.debug(f"new uncertainties: {uncertainties}")
    logging.info(f"new state ranges: {new_state_ranges}")
    # If a state range is full, then take all the states in that range, otherwise pick at random
    new_states = []
    for key, value in new_state_ranges.items():
        start_range = int(lambda_states["state_to_idx"][key[0]])
        end_range = int(lambda_states["state_to_idx"][key[1]])
        logging.debug(
            f"start range: {start_range}: {key[0]}, end range: {end_range}: {key[1]}"
        )

        if value == 1:
            # Take a state from the middle of the range
            idx = str(int((end_range - start_range) / 2) + start_range)
            next_state = lambda_states["idx_to_state"][idx]
            logging.debug(f"idx: {idx}, next state: {next_state}")
            new_states.append(next_state)
        else:
            # Take the values roughly evenly spaced in the range
            splits = np.array_split(np.arange(start_range + 1, end_range), value + 1)
            for i in range(value):
                idx = str(splits[i][-1])
                next_state = lambda_states["idx_to_state"][idx]
                logging.debug(f"idx: {idx}, next state: {next_state}")
                new_states.append(next_state)

    gro_info = {}
    for gro_dict in gro_states_dict:
        for state, gro in gro_dict.items():
            gro_info[state] = gro

    new_gro_files = []
    new_states_dict = {}
    # Find the gro file with the closest lambda value to the new state without going over
    for state in new_states:
        state_idx = lambda_states["state_to_idx"][state]
        new_states_dict[state_idx] = state
        logging.info(f"state: {state}, state index: {state_idx}")
        closest_state = max(
            (s for s in gro_info.keys() if float(s) <= float(state)),
            key=lambda x: float(x),
        )
        logging.info(f"closest state: {closest_state}")
        new_gro_fn = f"lambda_{state}.gro"
        new_gro_files.append({new_gro_fn: gro_info[closest_state]})
    return {"gro_files": new_gro_files, "new_states": new_states_dict}


@task
def next_step_mdp_options(next_step_info, lambda_states, **context):
    """
    Remove the state with the lowest uncertainty and add a new state between the states with the highest uncertainty.
    """
    import logging

    logging.debug(f"next step info: {next_step_info}")
    params = context["params"]
    num_steps = params["num_steps"]
    lambda_dirs = [
        f"lambda_{state}" for state in list(next_step_info["new_states"].values())
    ]
    logging.info(f"lambda dirs: {lambda_dirs}")
    new_mdp_options = []
    for idx, state in next_step_info["new_states"].items():
        logging.info(f"Adding state {state} with index {idx} to mdp options")
        new_mdp_options.append(
            {
                "init-lambda-state": idx,
                "nsteps": num_steps,
                "vdw_lambdas": lambda_states["states"],
            }
        )
    return new_mdp_options


@task
def copy_gro_files(gro_fn, output_dir, states_dict, lambda_states_per_step):
    import logging, os, shutil
    import numpy as np

    assert isinstance(output_dir, str)
    if not os.path.isabs(output_dir):
        output_dir = os.path.abspath(output_dir)
    logging.info(f"Copying gro files to {output_dir}")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    assert lambda_states_per_step >= 2
    states_arrays = np.array_split(
        np.array(list(states_dict.items())), lambda_states_per_step - 1
    )
    state_idxs, states = [], []
    for i, state in enumerate(states_arrays):
        if i == 0:
            state_idxs.append(state[0][0])
            state_idxs.append(state[-1][0])
            states.append(state[0][1])
            states.append(state[-1][1])
        else:
            state_idxs.append(state[-1][0])
            states.append(state[-1][1])

    for state in states:
        logging.info(f"Copying {gro_fn} to {output_dir} with state {state}")
        new_gro_path = os.path.join(output_dir, f"lambda_{state}.gro")
        shutil.copy(gro_fn, new_gro_path)
    return {
        "gro_dir": output_dir,
        "gro_files": [f"lambda_{state}.gro" for state in states],
        "states": states,
        "state_idxs": state_idxs,
    }


@task
def new_gro_paths(gro_updates, new_gro_dir, dataset_dict):
    import os, shutil, logging

    # gro_states = [gro for gro in dataset_dict]
    gro_paths = gro_updates["gro_files"]
    logging.info(f"gro paths: {gro_paths}")
    if not os.path.isabs(new_gro_dir):
        new_gro_dir = os.path.abspath(new_gro_dir)
    if not os.path.exists(new_gro_dir):
        os.makedirs(new_gro_dir)

    new_gro_paths = []
    for item in gro_paths:
        gro_fn = list(item.keys())[0]
        gro_to_copy = list(item.values())[0]
        gro_path = os.path.join(new_gro_dir, gro_fn)
        logging.info(f"Copying {gro_to_copy} to {gro_path}")
        shutil.copy(gro_to_copy, gro_path)
        new_gro_paths.append(os.path.basename(gro_path))
    return {
        "gro_dir": new_gro_dir,
        "gro_files": new_gro_paths,
        "state_idxs": list(gro_updates["new_states"].keys()),
        "states": list(gro_updates["new_states"].values()),
    }


@task(trigger_rule="none_failed_min_one_success")
def update_params(
    gro_update_init,
    gro_update_continue,
    mdp_params_continue,
    vdw_lambda_states=None,
    **context,
):
    import logging

    params = context["params"]
    if gro_update_init:
        logging.info(f"gro update init: {gro_update_init}")
        params_update = gro_update_init
    elif gro_update_continue:
        logging.info(f"gro update continue: {gro_update_continue}")
        params_update = gro_update_continue
    else:
        raise ValueError("No parameters to update")
    params["inputs"]["gro"]["directory"] = params_update["gro_dir"]
    params["inputs"]["gro"]["filename"] = params_update["gro_files"]

    num_steps = params["num_steps"]
    if mdp_params_continue:
        logging.info(f"mdp params continue: {mdp_params_continue}")

    params["mdp_options"] = []
    params["lambda_dirs"] = []
    lambda_states_total = list(vdw_lambda_states.values())
    for idx in params_update["state_idxs"]:
        state = vdw_lambda_states[idx]
        logging.info(f"Adding state {state} with index {idx} to mdp options")
        params["mdp_options"].append(
            {
                "init-lambda-state": idx,
                "nsteps": num_steps,
                "vdw_lambdas": lambda_states_total,
            }
        )
        params["lambda_dirs"].append(f"lambda_{state}")
    return params


@task
def collect_iteration_data(**context):
    import os

    iteration = context["params"]["iteration"]
    output_dir = context["params"]["output_dir"]
    expected_output = context["params"]["expected_output"]
    previous_dataset_paths = [
        os.path.join(output_dir, f"iteration_{i}", expected_output)
        for i in range(iteration)
    ]
    assert [os.path.exists(path) for path in previous_dataset_paths]
    return previous_dataset_paths


def map_dhdls(dhdl_data):
    return [(item["lambda_state"], item["dhdl"]) for item in dhdl_data]


def map_gros(dhdl_data):
    gro_dict = {}
    for item in dhdl_data:
        gro_dict[item["lambda_state"]] = item["gro"]
    return gro_dict


@task_group
def do_TI(dhdl, gro, states, output_dir, states_per_step):
    ti = TI.override(task_id="TI")(
        dhdl_data=dhdl,
        output_dir=output_dir,
    )
    ti_results = get_new_state.override(task_id="get_ti_states")(
        results=ti,
        gro_states_dict=gro,
        lambda_states=states,
        states_per_step=states_per_step,
        method="TI",
    )
    ti_next_step_mdp = next_step_mdp_options.override(task_id="make_next_mdp_ti")(
        next_step_info=ti_results,
        lambda_states=states,
    )
    return ti_results, ti_next_step_mdp


@task_group
def do_MBAR(dhdl, gro, states, output_dir, states_per_step):
    mbar = MBAR.override(task_id="MBAR")(
        dhdl_data=dhdl,
        output_dir=output_dir,
    )
    mbar_results = get_new_state.override(task_id="get_mbar_states")(
        results=mbar,
        gro_states_dict=gro,
        lambda_states=states,
        states_per_step=states_per_step,
        method="MBAR",
    )
    mbar_next_step_mdp = next_step_mdp_options.override(task_id="make_next_mdp_mbar")(
        next_step_info=mbar_results,
        lambda_states=states,
    )
    return mbar_results, mbar_next_step_mdp


with DAG(
    dag_id="anthracene_files_simple",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params=dagrun_params,
) as anthacene_files:
    get_states = generate_lambda_states("{{ params.lambda_states_total }}")
    gro_init = get_file.override(task_id="get_gro_init")(
        input_dir="{{ params.inputs.gro.directory }}",
        use_ref_data=True,
        file_name="{{ params.inputs.gro.filename }}",
    )
    copy_gro_init = copy_gro_files.override(task_id="copy_gro_files_init")(
        gro_fn=gro_init,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}/inputs",
        states_dict=get_states["idx_to_state"],
        lambda_states_per_step="{{ params.lambda_states_per_step }}",
    )

    prev_iter_datasets = collect_iteration_data.override(
        task_id="collect_iteration_data"
    )()
    prev_iter_data = json_from_dataset_path.override(task_id="get_data").expand(
        dataset_path=prev_iter_datasets,
    )
    dhdl_data = prev_iter_data.map(map_dhdls)
    gro_data = prev_iter_data.map(map_gros)
    prev_iter_datasets >> prev_iter_data

    do_TI(
        dhdl_data,
        gro_data,
        get_states,
        "{{ params.output_dir }}/iteration_{{ params.iteration - 1 }}",
        "{{ params.lambda_states_per_step }}",
    )
    mbar_results, mbar_next_step_mdp = do_MBAR(
        dhdl_data,
        gro_data,
        get_states,
        "{{ params.output_dir }}/iteration_{{ params.iteration - 1 }}",
        "{{ params.lambda_states_per_step }}",
    )

    gro_branch_task = branch_task_template.override(task_id="gro_branch")(
        statement="{{ params.iteration }} == 0",
        task_if_true="get_gro_init",
        task_if_false="collect_iteration_data",
    )
    gro_branch_task >> [gro_init, prev_iter_datasets]

    copy_gro_continue = new_gro_paths.override(task_id="copy_gro_files_continue")(
        gro_updates=mbar_results,
        new_gro_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}/inputs",
        dataset_dict=gro_data,
    )

    new_params = update_params.override(task_id="update_params")(
        gro_update_init=copy_gro_init,
        gro_update_continue=copy_gro_continue,
        mdp_params_continue=mbar_next_step_mdp,
        vdw_lambda_states=get_states["idx_to_state"],
    )

    is_anthracene_done = get_file.override(task_id="is_anthracene_done")(
        input_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        file_name="{{ params.expected_output }}",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_anthracene = TriggerDagRunOperator(
        task_id="trigger_anthracene",
        trigger_dag_id="anthracene_simple",
        wait_for_completion=True,
        poke_interval=2,
        trigger_rule="none_failed",
        conf=new_params,
    )
    anthracene_done = EmptyOperator(task_id="this_iteration_done")
    anthracene_done_branch = branch_task.override(task_id="anthracene_done_branch")(
        truth_value=is_anthracene_done,
        task_if_true="this_iteration_done",
        task_if_false="trigger_anthracene",
    )
    next_iteration_params = {
        "iteration": "{{ params.iteration + 1 }}",
        "output_dir": "{{ params.output_dir }}",
        "output_name": "{{ params.output_name }}",
        "expected_output": "{{ params.expected_output }}",
        "num_steps": "{{ params.num_steps }}",
        "max_iterations": "{{ params.max_iterations }}",
        "output_dataset_structure": "{{ params.output_dataset_structure }}",
        "lambda_states_per_step": "{{ params.lambda_states_per_step }}",
        "lambda_states_total": "{{ params.lambda_states_total }}",
    }
    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed_min_one_success"
    )(
        statement="{{ params.iteration }} >= {{ params.max_iterations }}",
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="anthracene_files_simple",
        dag_params=next_iteration_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )
    (
        new_params
        >> anthracene_done_branch
        >> [trigger_anthracene, anthracene_done]
        >> do_next_iteration
    )
