from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils import timezone
from airflowHPC.dags.tasks import (
    get_file,
    run_if_false,
    evaluate_template_truth,
    dict_from_xcom_dicts,
)
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflowHPC.operators.external_output_task import ExternalTaskOutputSensor
from airflow.models import DagRun
from airflow import settings


@task_group
def run_iteration(
    mdp, top, gro, output_dir, output_name, iteration_num, num_simulations, coul_lambda
):
    from airflowHPC.operators.resource_gmx_operator import ResourceGmxOperatorDataclass
    from airflowHPC.dags.tasks import prepare_gmx_input_deep, update_gmxapi_input

    grompp_input_list = prepare_gmx_input_deep.override(task_id="grompp_input_list")(
        args=["grompp"],
        input_files={"-f": mdp, "-c": gro, "-p": top},
        output_files={"-o": f"{output_name}.tpr"},
        output_dir_outer=output_dir,
        output_dir_inner=f"lambda_{coul_lambda}",
        counter=iteration_num,
        num_simulations=num_simulations,
    )

    grompp = ResourceGmxOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list)
    grompp_input_list >> grompp

    mdrun_input_list = (
        update_gmxapi_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", output_name, "-ntomp", "2"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-c": f"{output_name}.gro",
                "-x": f"{output_name}.xtc",
                "-dhdl": "dhdl.xvg",
            },
        )
        .expand(gmxapi_output=grompp.output)
    )
    mdrun = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
    grompp >> mdrun

    dataset_dict = dict_from_xcom_dicts.override(task_id="make_dataset_dict")(
        list_of_dicts="{{task_instance.xcom_pull(task_ids='run_iteration.mdrun_input_list')}}",
        dict_structure={
            "simulation_id": "simulation_id",
            "tpr": "-s",
            "xtc": "-x",
            "gro": "-c",
        },
    )
    mdrun_input_list >> dataset_dict >> mdrun


with DAG(
    "gmx_triggered",
    start_date=timezone.utcnow(),
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
    params={
        "output_dir": "outputs",
        "output_name": "sim",
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "basic_md.json"},
            "gro": {"directory": "ensemble_md", "filename": "sys.gro"},
            "top": {"directory": "ensemble_md", "filename": "sys.top"},
            "coul_lambda": 0,
        },
        "num_sims": 4,
        "iteration_num": 0,
    },
) as callee_dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_sim = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update")(
        mdp_json_file_path=input_mdp,
        update_dict={"nsteps": 12500},
    )

    run_iteration(
        mdp=mdp_sim,
        top=input_top,
        gro=input_gro,
        output_dir="{{ params.output_dir }}",
        output_name="{{ params.output_name }}",
        iteration_num="{{ params.iteration_num }}",
        num_simulations="{{ params.num_sims }}",
        coul_lambda="{{ params.inputs.coul_lambda }}",
    )


@task
def num_lambda_points():
    import random

    return random.randint(2, 3)


@task
def gen_configurations(
    output_dir, output_name, mdp, gro, top, num_sims, iteration_num, num_lambda_points
):
    import numpy as np

    lambda_points = np.random.rand(num_lambda_points)
    configs = [
        {
            "output_dir": output_dir,
            "output_name": output_name,
            "inputs": {
                "mdp": mdp,
                "gro": gro,
                "top": top,
                "coul_lambda": lambda_point,
            },
            "num_sims": num_sims,
            "iteration_num": iteration_num,
        }
        for lambda_point in lambda_points
    ]
    return configs


@task
def simulation_time(files):
    import logging
    import os
    import MDAnalysis as mda

    sim_time: float = 0
    for out_file in files:
        tpr, xtc = out_file["tpr"], out_file["xtc"]
        if os.path.exists(tpr) and os.path.exists(xtc):
            time = mda.Universe(tpr, xtc).trajectory.totaltime
        else:
            time = 0
        logging.info(f"Simulation time for {out_file['xtc']} is {time}")
        sim_time += time
    return sim_time


@task
def random_gro(files):
    import random
    import os

    gro = os.path.abspath(random.choice(files)["gro"])
    return {"directory": os.path.dirname(gro), "filename": os.path.basename(gro)}


def get_execution_date(_, context):
    session = settings.Session()
    dr = (
        session.query(DagRun)
        .filter(DagRun.dag_id == context["task"].external_dag_id)
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    return dr.execution_date


with DAG(
    "gmx_triggerer",
    start_date=timezone.utcnow(),
    render_template_as_native_obj=True,
    params={
        "output_dir": "outputs",
        "output_name": "sim",
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "basic_md.json"},
            "gro": {"directory": "ensemble_md", "filename": "sys.gro"},
            "top": {"directory": "ensemble_md", "filename": "sys.top"},
        },
        "num_sims": 2,
        "iteration_num": 1,
        "iterations_to_run": 2,
        "branch_ps_wait_time": 81,
    },
) as caller_dag:
    num_lambdas = num_lambda_points()
    configs_to_run = gen_configurations(
        output_dir="{{ params.output_dir }}",
        output_name="{{ params.output_name }}",
        mdp="{{ params.inputs.mdp }}",
        gro="{{ params.inputs.gro }}",
        top="{{ params.inputs.top }}",
        num_sims="{{ params.num_sims }}",
        iteration_num="{{ params.iteration_num }}",
        num_lambda_points=num_lambdas,
    )
    run_steps = TriggerDagRunOperator.partial(
        task_id=f"triggering",
        trigger_dag_id="gmx_triggered",
        poke_interval=2,
    ).expand(conf=configs_to_run)

    wait_for_data = ExternalTaskOutputSensor(
        task_id="wait_for_data_paths",
        external_dag_id="gmx_triggered",
        external_task_id="run_iteration.make_dataset_dict",
        execution_date_fn=get_execution_date,
        mode="poke",
        poke_interval=5,
        check_existence=True,
    )
    wait_for_run = ExternalTaskSensor(
        task_id="wait_for_mdrun",
        external_dag_id="gmx_triggered",
        external_task_group_id="run_iteration",
        execution_date_fn=get_execution_date,
        mode="poke",
        poke_interval=5,
        check_existence=True,
        failed_states=["failed"],
    )
    total_time = simulation_time.override(task_id="total_time")(
        files="{{ task_instance.xcom_pull(task_ids='wait_for_data_paths', key='return_value') }}"
    )
    do_separate_branch = evaluate_template_truth.override(
        task_id="do_separate_branch", trigger_rule="none_failed"
    )(
        statement="{{ params.branch_ps_wait_time }} > {{ task_instance.xcom_pull(task_ids='total_time') }}"
    )
    separate_gro = random_gro.override(task_id="separate_gro")(
        files="{{ task_instance.xcom_pull(task_ids='wait_for_data_paths', key='return_value') }}"
    )
    separate_branch_params = {
        "output_dir": "{{ params.output_dir }}/separate",
        "output_name": "{{ params.output_name }}",
        "inputs": {
            "mdp": "{{ params.inputs.mdp }}",
            "gro": separate_gro,
            "top": "{{ params.inputs.top }}",
        },
        "num_sims": 2,
        "iteration_num": 1,
        "iterations_to_run": 1,
        "branch_ps_wait_time": 500,
    }
    separate_branch = run_if_false.override(group_id="separate_branch")(
        dag_id="gmx_triggerer",
        dag_params=separate_branch_params,
        truth_value=do_separate_branch,
        wait_for_completion=False,
    )
    run_steps >> wait_for_data >> total_time >> separate_gro >> separate_branch
    total_time >> do_separate_branch >> separate_branch

    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed"
    )(statement="{{ params.iteration_num }} >= {{ params.iterations_to_run }}")
    next_gro = random_gro.override(task_id="next_gro")(
        files="{{ task_instance.xcom_pull(task_ids='wait_for_data_paths', key='return_value') }}"
    )
    next_iteration_params = {
        "output_dir": "{{ params.output_dir }}",
        "output_name": "{{ params.output_name }}",
        "inputs": {
            "mdp": "{{ params.inputs.mdp }}",
            "gro": next_gro,
            "top": "{{ params.inputs.top }}",
        },
        "num_sims": "{{ params.num_sims }}",
        "iteration_num": "{{ params.iteration_num + 1 }}",
        "iterations_to_run": "{{ params.iterations_to_run }}",
        "branch_ps_wait_time": "{{ params.branch_ps_wait_time }}",
    }
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="gmx_triggerer",
        dag_params=next_iteration_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )

    run_steps >> wait_for_run >> next_gro >> next_iteration
    wait_for_run >> do_next_iteration >> next_iteration
