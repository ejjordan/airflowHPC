from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_if_false, evaluate_template_truth
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task_group
def run_iteration(
    mdp, top, gro, output_dir, output_name, iteration_num, num_simulations, coul_lambda
):
    from airflowHPC.operators.resource_gmx_operator import ResourceGmxOperatorDataclass
    from airflowHPC.dags.tasks import prepare_gmx_input_deep, update_gmxapi_input
    import logging

    logging.info(f"run_iteration: coul_lambda: {coul_lambda}")

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
            output_files={"-c": f"{output_name}.gro", "-dhdl": "dhdl.xvg"},
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
    return mdrun


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
        update_dict={"nsteps": 10000},
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

    return random.randint(2, 4)


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


@task(multiple_outputs=True)
def next_iteration(params):
    params["iteration_num"] += 1
    return params


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
        "num_sims": 4,
        "iteration_num": 1,
        "iterations_to_run": 2,
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
        wait_for_completion=True,
        poke_interval=2,
    ).expand(conf=configs_to_run)

    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed"
    )(statement="{{ params.iteration_num }} >= {{ params.iterations_to_run }}")
    next_iteration_params = next_iteration.override(task_id="next_iteration_params")(
        params="{{ params }}"
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="gmx_triggerer",
        dag_params=next_iteration_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )

    run_steps >> do_next_iteration >> next_iteration
