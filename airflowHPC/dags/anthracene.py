from typing import Literal
from airflow import DAG
from airflow.decorators import task
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
                "filename": [
                    "lam000.gro",
                    "lam010.gro",
                    "lam020.gro",
                    "lam030.gro",
                    "lam040.gro",
                    "lam050.gro",
                    "lam060.gro",
                    "lam070.gro",
                    "lam080.gro",
                    "lam090.gro",
                    "lam100.gro",
                ],
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
    "mdp_options": [
        {"init-lambda-state": 0, "nsteps": 10000},
        {"init-lambda-state": 10},
        {"init-lambda-state": 20},
        {"init-lambda-state": 30},
        {"init-lambda-state": 40},
        {"init-lambda-state": 50},
        {"init-lambda-state": 60},
        {"init-lambda-state": 70},
        {"init-lambda-state": 80},
        {"init-lambda-state": 90},
        {"init-lambda-state": 100, "nsteps": 10000},
    ],
    "output_dir": "anthracene",
    "output_name": "anthra",
    "expected_output": "anthra.json",
    "iteration": 0,
    "output_dataset_structure": {
        "dhdl": "-dhdl",
        "gro": "-c",
    },
}


@task
def get_lambdas_from_mdp(
    mdp_json_path: dict,
    mdp_json_key: str,
    mdp_options: list,
    mdp_options_key: str,
    prefix: str = "",
):
    import json

    opt_keys = [d[mdp_options_key] for d in mdp_options]
    with open(mdp_json_path, "r") as mdp_file:
        mdp_json = json.load(mdp_file)
    lambda_vals = mdp_json[mdp_json_key].split()
    return [f"{prefix}{lambda_vals[i]}" for i in opt_keys]


@task
def add_lambdas_to_dataset(dataset_path: str, keys_list: list):
    import json

    with open(dataset_path, "r") as dataset_file:
        dataset = json.load(dataset_file)
    seen_keys = []
    for key in keys_list:
        for i, data in enumerate(dataset):
            if key in data["gro"]:
                dataset[i]["lambda_state"] = key
                seen_keys.append(key)
                break
    assert len(seen_keys) == len(keys_list)
    with open(dataset_path, "w") as dataset_file:
        json.dump(dataset, dataset_file, indent=2, separators=(",", ": "))
    return dataset


with DAG(
    dag_id="anthracene",
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
        .partial(input_dir="{{ params.inputs.gro.directory }}", use_ref_data=True)
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
    lambda_dirs = get_lambdas_from_mdp.override(task_id="get_lambda_dirs")(
        mdp_json, "vdw_lambdas", mdp_options, "init-lambda-state", prefix="lambda_"
    )
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
    dhdl_files,
    output_dir,
    temp: float = 300.0,
    equil_index: int = 10,
    states: list = None,
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

    logging.info(f"lambda values: {states}")
    state_uncertainties = []
    for i, state in enumerate(states[:-1]):
        neighbor_uncertainty = ti.d_delta_f_.loc[float(state), float(states[i + 1])]
        logging.info(
            f"Uncertainty between lambda {state} and lambda {states[i + 1]}: {neighbor_uncertainty}"
        )
        state_uncertainties.append((state, states[i + 1], neighbor_uncertainty))
    max_uncertainty = max(state_uncertainties, key=lambda x: x[2])
    min_uncertainty = min(state_uncertainties[1:], key=lambda x: x[2])

    # Plotting
    ax = plot_ti_dhdl(ti, labels=["VDW"], colors="r")
    ax.figure.savefig(os.path.join(output_dir, "dhdl_TI.png"))
    return {"max": max_uncertainty, "min": min_uncertainty}


@task
def MBAR(
    dhdl_files,
    output_dir,
    temp: float = 300.0,
    equil_index: int = 10,
    states: list = None,
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

    logging.info(f"lambda values: {states}")
    state_uncertainties = []
    for i, state in enumerate(states[:-1]):
        neighbor_uncertainty = mbar.d_delta_f_.loc[float(state), float(states[i + 1])]
        logging.info(
            f"Uncertainty between lambda {state} and lambda {states[i+1]}: {neighbor_uncertainty}"
        )
        state_uncertainties.append((state, states[i + 1], neighbor_uncertainty))
    max_uncertainty = max(state_uncertainties, key=lambda x: x[2])
    min_uncertainty = min(state_uncertainties[1:], key=lambda x: x[2])

    # Plotting
    ax = plot_mbar_overlap_matrix(mbar.overlap_matrix)
    ax.figure.savefig(
        os.path.join(output_dir, "O_MBAR.png"), bbox_inches="tight", pad_inches=0.0
    )
    return {"max": max_uncertainty, "min": min_uncertainty}


@task(multiple_outputs=True)
def generate_lambda_states(num_states: int):
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
def get_new_state(results, lambda_states, method: Literal["TI", "MBAR"], **context):
    import logging
    import random

    min_state1 = results["min"][0]
    min_state2 = results["min"][1]
    min_uncertainty = results["min"][2]
    removal_state_idx = int(lambda_states["state_to_idx"][min_state1])
    assert float(min_state1) > 0

    logging.info(
        f"{method} min uncertainty between states {min_state1} and {min_state2}: {min_uncertainty}"
    )
    logging.info(f"Will remove state: {removal_state_idx}")

    max_state1 = results["max"][0]
    max_state2 = results["max"][1]
    max_uncertainty = results["max"][2]
    max_state_idx1 = lambda_states["state_to_idx"][max_state1]
    max_state_idx2 = lambda_states["state_to_idx"][max_state2]
    assert max_state_idx1 < max_state_idx2
    new_state_idx = random.randrange(int(max_state_idx1) + 1, int(max_state_idx2) - 1)

    params = context["params"]
    logging.info(
        f"{method} max uncertainty between states {max_state1} and {max_state2}: {max_uncertainty}"
    )
    logging.info(
        f"{method} max uncertainty state indices: {max_state_idx1} and {max_state_idx2}"
    )
    logging.info(
        f"new {method} state: {new_state_idx}: {lambda_states['idx_to_state'][str(new_state_idx)]}"
    )

    # Add new state to gro files. Pick the gro file with the closest lambda value to the new state, without going over
    gro_files = params["inputs"]["gro"]["filename"]
    for i, gro in enumerate(gro_files):
        if float(new_state_idx) < float(gro[3:6]):
            gro_to_copy = gro_files[i - 1]
            break
    logging.info(f"gro file to copy: {gro_to_copy}")
    return {
        "gro_to_copy": gro_to_copy,
        "gro_to_copy_idx": int(gro_to_copy[3:6]),
        "new_gro_idx": new_state_idx,
        "new_gro_fn": f"lam{new_state_idx:03d}.gro",
        "gro_to_remove": f"lam{removal_state_idx:03d}.gro",
        "gro_to_remove_idx": removal_state_idx,
    }


@task
def next_step_mdp_options(next_step_info, **context):
    """
    Remove the state with the lowest uncertainty and add a new state between the states with the highest uncertainty.
    """
    import ast

    params = context["params"]
    new_mdp_options = [
        ast.literal_eval(opt) if type(opt) is str else opt
        for opt in params["mdp_options"]
    ]

    new_mdp_options = [
        opt
        for opt in new_mdp_options
        if opt["init-lambda-state"] != next_step_info["gro_to_remove_idx"]
    ]
    new_mdp_options.append({"init-lambda-state": next_step_info["new_gro_idx"]})
    new_mdp_options = sorted(new_mdp_options, key=lambda x: x["init-lambda-state"])
    return new_mdp_options


@task
def copy_gro_files(gro_files, output_dir):
    import logging, os, shutil

    assert isinstance(output_dir, str)
    if not os.path.isabs(output_dir):
        output_dir = os.path.abspath(output_dir)
    logging.info(f"Copying gro files to {output_dir}")
    logging.info(f"gro_files: {[gro for gro in gro_files]}")
    output_dirs_list = [output_dir] * len(gro_files)
    for gro_file, dir in zip(gro_files, output_dirs_list):
        logging.info(f"Copying {gro_file} to {dir}")
        if not os.path.exists(dir):
            os.makedirs(dir)
        shutil.copy(gro_file, dir)
    return {
        "gro_dir": output_dir,
        "gro_files": [os.path.basename(f) for f in gro_files],
    }


@task
def new_gro_paths(gro_updates, new_gro_dir, dataset_dict):
    import os, shutil, logging

    gro_paths = []
    for item in dataset_dict:
        gro_path = item["gro"]
        lambda_state = item["lambda_state"]
        lambda_state = "".join(lambda_state.strip("lambda_").split("."))
        if int(lambda_state) == (gro_updates["gro_to_remove_idx"]):
            continue
        elif int(lambda_state) == int(gro_updates["gro_to_copy_idx"]):
            gro_paths.append(
                (gro_path, os.path.join(new_gro_dir, f"lam{lambda_state}.gro"))
            )
            gro_paths.append(
                (gro_path, os.path.join(new_gro_dir, gro_updates["new_gro_fn"]))
            )
        else:
            gro_paths.append(
                (gro_path, os.path.join(new_gro_dir, f"lam{lambda_state}.gro"))
            )
    for path in gro_paths:
        logging.info(f"Copying {path[0]} to {path[1]}")
        if not os.path.exists(new_gro_dir):
            os.makedirs(new_gro_dir)
        shutil.copy(path[0], path[1])
    return {
        "gro_dir": new_gro_dir,
        "gro_files": [os.path.basename(f[1]) for f in gro_paths],
    }


@task(trigger_rule="none_failed_min_one_success")
def update_params(params_update_init, params_update_continue, **context):
    params = context["params"]
    if params_update_init:
        params_update = params_update_init
    elif params_update_continue:
        params_update = params_update_continue
    else:
        raise ValueError("No parameters to update")
    params["inputs"]["gro"]["directory"] = params_update["gro_dir"]
    params["inputs"]["gro"]["filename"] = params_update["gro_files"]

    return params


from copy import deepcopy

fh_params = deepcopy(dagrun_params)
fh_params["iteration"] = 0
gro_updates = {
    "gro_to_copy": "lam070.gro",
    "gro_to_copy_idx": 70,
    "new_gro_idx": 76,
    "new_gro_fn": "lam076.gro",
    "gro_to_remove": "lam020.gro",
    "gro_to_remove_idx": 20,
}
with DAG(
    dag_id="anthracene_files",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params=fh_params,
) as anthacene_files:
    gro_files_list_init = unpack_param.override(task_id="get_gro_files_list_init")(
        "{{ params.inputs.gro.filename }}"
    )
    gro_files_init = (
        get_file.override(task_id="get_gro_files_init")
        .partial(input_dir="{{ params.inputs.gro.directory }}", use_ref_data=True)
        .expand(file_name=gro_files_list_init)
    )
    mapped_gro_files_init = gro_files_init.map(lambda x: x)
    copy_gro_init = copy_gro_files.override(task_id="copy_gro_files_init")(
        gro_files=mapped_gro_files_init,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}/inputs",
    )

    prev_iter_dataset = get_file.override(task_id="get_dataset")(
        input_dir="{{ params.output_dir }}/iteration_{{ params.iteration - 1 }}",
        file_name="{{ params.expected_output }}",
        use_ref_data=False,
        check_exists=False,
    )
    prev_iter_data = json_from_dataset_path.override(
        task_id="get_data", trigger_rule="all_success"
    )(
        dataset_path="{{ params.output_dir }}/iteration_{{ params.iteration - 1 }}/{{ params.expected_output }}",
    )
    copy_gro_continue = new_gro_paths.override(task_id="copy_gro_files_continue")(
        gro_updates=gro_updates,
        new_gro_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}/inputs",
        dataset_dict=prev_iter_data,
    )
    prev_iter_dataset >> prev_iter_data

    gro_branch_task = branch_task_template.override(task_id="gro_branch")(
        statement="{{ params.iteration }} == 0",
        task_if_true="get_gro_files_list_init",
        task_if_false="get_dataset",
    )
    gro_branch_task >> [gro_files_list_init, prev_iter_dataset]

    new_params = update_params.override(task_id="update_params")(
        params_update_init=copy_gro_init, params_update_continue=copy_gro_continue
    )


with DAG(
    dag_id="anthracene_runner",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params=dagrun_params,
) as run_anthacene:
    is_anthracene_done = get_file.override(task_id="is_anthracene_done")(
        input_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        file_name="{{ params.expected_output }}",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_anthracene = TriggerDagRunOperator(
        task_id="trigger_anthracene",
        trigger_dag_id="anthracene",
        wait_for_completion=True,
        poke_interval=2,
        trigger_rule="none_failed",
        conf="{{ params }}",
    )
    anthracene_done_branch = branch_task.override(task_id="anthracene_done_branch")(
        truth_value=is_anthracene_done,
        task_if_true="get_dhdl_files",
        task_if_false="trigger_anthracene",
    )

    get_dhdl_files = json_from_dataset_path.override(
        task_id="get_dhdl_files", trigger_rule="none_failed"
    )(
        dataset_path="{{ params.output_dir }}/iteration_{{ params.iteration }}/{{ params.expected_output }}",
        key="dhdl",
    )
    mdp_options = unpack_mdp_options()
    mdp_json = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    lambda_vals = get_lambdas_from_mdp.override(task_id="get_states")(
        mdp_json, "vdw_lambdas", mdp_options, "init-lambda-state"
    )
    ti = TI.override(task_id="TI")(
        dhdl_files=get_dhdl_files,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        states=lambda_vals,
    )
    mbar = MBAR.override(task_id="MBAR")(
        dhdl_files=get_dhdl_files,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        states=lambda_vals,
    )
    get_states = generate_lambda_states(101)
    mbar_results = get_new_state.override(task_id="get_mbar_states")(
        results=mbar,
        lambda_states=get_states,
        method="MBAR",
    )
    mbar_next_step_mdp = next_step_mdp_options.override(task_id="make_next_mdp_mbar")(
        next_step_info=mbar_results
    )

    ti_results = get_new_state.override(task_id="get_ti_states")(
        results=ti,
        lambda_states=get_states,
        method="TI",
    )
    ti_next_step_mdp = next_step_mdp_options.override(task_id="make_next_mdp_ti")(
        next_step_info=ti_results
    )

    is_anthracene_done >> anthracene_done_branch >> [trigger_anthracene, get_dhdl_files]
    trigger_anthracene >> get_dhdl_files
    get_dhdl_files >> [ti, mbar]
    ti >> ti_results >> ti_next_step_mdp
    mbar >> mbar_results >> mbar_next_step_mdp
