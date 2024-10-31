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
                    "lam00.gro",
                    "lam01.gro",
                    "lam02.gro",
                    "lam03.gro",
                    "lam04.gro",
                    "lam05.gro",
                    "lam06.gro",
                    "lam07.gro",
                    "lam08.gro",
                    "lam09.gro",
                    "lam10.gro",
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


with DAG(
    dag_id="anthracene",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
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
    lambda_vals = get_lambdas_from_mdp(
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
        names=lambda_vals,
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
    mdrun >> dataset


@task
def TI(dhdl_files, output_dir, temp: float = 300.0, equil_index: int = 10):
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

    # Plotting
    ax = plot_ti_dhdl(ti, labels=["VDW"], colors="r")
    ax.figure.savefig(os.path.join(output_dir, "dhdl_TI.png"))


@task
def MBAR(dhdl_files, output_dir, temp: float = 300.0, equil_index: int = 10):
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

    # Plotting
    ax = plot_mbar_overlap_matrix(mbar.overlap_matrix)
    ax.figure.savefig(
        os.path.join(output_dir, "O_MBAR.png"), bbox_inches="tight", pad_inches=0.0
    )


with DAG(
    dag_id="anthracene_runner",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
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
    ti = TI.override(task_id="TI")(
        dhdl_files=get_dhdl_files,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
    )
    mbar = MBAR.override(task_id="MBAR")(
        dhdl_files=get_dhdl_files,
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
    )
    is_anthracene_done >> anthracene_done_branch >> [trigger_anthracene, get_dhdl_files]
    trigger_anthracene >> get_dhdl_files
    get_dhdl_files >> [ti, mbar]
