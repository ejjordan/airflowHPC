from datetime import timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.utils.timezone import datetime

from airflowHPC.dags.tasks import (
    json_from_dataset_path,
    add_to_dataset,
    run_if_false,
)
from airflowHPC.dags.swarms_tasks import iterations_completed


@task(multiple_outputs=True)
def iteration_params(completed_iterations, previous_results, **context):
    import logging

    iteration_to_run = completed_iterations + 1
    iteration_to_run_name = f"iteration_{iteration_to_run}"
    if iteration_to_run_name in previous_results:
        iteration_data = previous_results[iteration_to_run_name]
        if "params" in iteration_data:
            dag_params = iteration_data["params"]
            dag_params["max_iterations"] = context["params"]["max_iterations"]
        else:
            logging.error(f"No params in {iteration_to_run_name}")
            raise AirflowException(f"No params in {iteration_to_run_name}")
    else:
        dag_params = context["params"]
    dag_params["iteration"] = iteration_to_run
    dag_params["output_dir"] = f"{dag_params['output_dir']}/{iteration_to_run_name}"
    dag_params["output_dataset_structure"] = {
        "simulation_id": "simulation_id",
        "gro": "-c",
        "tpr": "-s",
        "xtc": "-x",
    }
    return {
        "params": dag_params,
        "iteration": iteration_to_run,
        "is_first_iteration": iteration_to_run == 1,
        "trigger_this_iteration": iteration_to_run <= dag_params["max_iterations"],
        "output_dir": dag_params["output_dir"],
    }


@task.short_circuit
def short_circuit(condition: bool):
    return condition


@task(multiple_outputs=True)
def new_input(dag_params, new_gro_info, phi_psi_point, **context):
    import os
    import logging

    ti = context["ti"]
    map_index = ti.map_index
    new_output_dir = f"{dag_params['output_dir']}/{map_index}"
    logging.info(f"new_gro: {new_gro_info}")
    logging.info(f"dag_params: {dag_params}")
    new_gro = new_gro_info["next_gro"]
    gro_path, gro_fn = os.path.split(new_gro)
    dag_params["inputs"]["gro"]["directory"] = gro_path
    dag_params["inputs"]["gro"]["filename"] = gro_fn
    dag_params["inputs"]["gro"]["ref_data"] = False
    dag_params["output_dir"] = new_output_dir
    dag_params["point"] = phi_psi_point
    dag_params["num_sims"] = dag_params["swarm_size"]
    dag_params["mdp_options"] = dag_params["mdp_options"]["swarm"]
    dag_params["expected_output"] = f"{dag_params['output_files']['sim']}.gro"
    return {"params": dag_params, "output_dir": new_output_dir}


@task
def pull_params(
    phi_psi,
    output_path,
    iter_params,
    iteration,
    force_constant,
    expected_output,
    point_idx,
    **context,
):
    import ast
    import logging

    logging.info(f"phi_psi: {phi_psi}")
    phi = phi_psi[0]
    psi = phi_psi[1]
    if iteration > 1:
        point = iter_params["inputs"]["point"][point_idx]
        if type(point) == str:
            point = ast.literal_eval(point)
        assert phi_psi == point
    ti = context["ti"]
    map_index = ti.map_index
    output_dir = f"{output_path}/iteration_{iteration}/{map_index}/pull"
    # Use output from previous iteration, if available
    if iteration > 1:
        gro_path = iter_params["inputs"]["gro"]["directory"][point_idx]
        gro_fn = iter_params["inputs"]["gro"]["filename"][point_idx]
        ref_data = False
    else:
        gro_path = iter_params["inputs"]["gro"]["directory"]
        gro_fn = iter_params["inputs"]["gro"]["filename"]
        ref_data = True
    inputs = {
        "gro": {"directory": gro_path, "filename": gro_fn, "ref_data": ref_data},
        "top": iter_params["inputs"]["top"],
        "mdp": iter_params["inputs"]["mdp"],
    }
    return {
        "phi_angle": phi,
        "psi_angle": psi,
        "force_constant": force_constant,
        "output_dir": output_dir,
        "expected_output": expected_output,
        "inputs": inputs,
        "mdp_options": iter_params["mdp_options"]["pull"],
    }


@task(trigger_rule="none_failed")
def json_from_dataset_path_parts(
    dataset_path_parts: list[str], key: str = None, allow_missing: bool = False
):
    import json, os
    import logging

    logging.info(
        f"path parts types: {[(part, type(part)) for part in dataset_path_parts]}"
    )
    str_parts = [str(part) for part in dataset_path_parts]
    logging.info(f"str parts types: {[(part, type(part)) for part in str_parts]}")
    dataset_path = os.path.join(*str_parts)
    if not os.path.exists(dataset_path):
        if allow_missing:
            return {}
        else:
            raise AirflowException(f"Dataset path {dataset_path} does not exist")
    with open(dataset_path, "r") as f:
        data = json.load(f)
    if not key:
        return data
    else:
        if len(data) == 1:
            return data[str(key)]
        else:
            return [d[key] for d in data]


@task(multiple_outputs=True)
def compute_drift(phi_psi, sim_info, pull_info):
    import logging
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran
    import numpy as np

    # MDAnalysis is too verbose
    logging.getLogger("MDAnalysis").setLevel(logging.WARNING)

    logging.info(f"phi_psi: {phi_psi}")
    initial_point = pull_info["next_gro_means"]
    logging.info(f"initial_point: {initial_point}")
    first_frame_means = []
    last_frames_means = []
    for sim_data in sim_info:
        tpr = sim_data["tpr"]
        xtc = sim_data["xtc"]
        u = Universe(tpr, xtc)
        selected_residues = u.select_atoms("protein")
        rama = Ramachandran(selected_residues).run()
        angles = np.round(rama.results.angles, 3)
        angle_means = np.round(np.mean(angles, axis=1), 2).tolist()
        logging.info(f"num angles: {len(angles[0])}")
        logging.info(f"frame first mean:  {angle_means[0]}")
        logging.info(f"frame last mean:  {angle_means[-1]}")
        first_frame_means.append(angle_means[0])
        last_frames_means.append(angle_means[-1])

    first_mean = np.round(np.mean(first_frame_means, axis=0), 2)
    last_mean = np.round(np.mean(last_frames_means, axis=0), 2)
    logging.info(f"cumulative first mean: {first_mean}")
    logging.info(f"cumulative last mean: {last_mean}")

    # find the last frame closest to last mean
    closest_sim_idx = np.argmin(
        [np.linalg.norm(x - last_mean) for x in np.array(last_frames_means)]
    )
    best_gro = sim_info[closest_sim_idx]["gro"]

    return {
        "initial_point_means": first_mean.tolist(),
        "final_point_means": last_mean.tolist(),
        "phi_psi_points": phi_psi,
        "final_points": last_frames_means,
        "best_gro": best_gro,
    }


@task_group
def step(
    phi_psi,
    output_dir,
    iter_params,
    iteration,
    force_constant,
    swarm_expected_output,
    sim_expected_output,
    pull_expected_output,
):
    pull_dag_params = pull_params(
        phi_psi=phi_psi,
        output_path=output_dir,
        iter_params=iter_params,
        iteration=iteration,
        force_constant=force_constant,
        expected_output=pull_expected_output,
        point_idx="{{ task_instance.map_index }}",
    )
    trigger_pull_dag = TriggerDagRunOperator(
        task_id="trigger_pull_dag",
        trigger_dag_id="pull",
        trigger_run_id="{{ task_instance.map_index }}_{{ task_instance.try_number }}_{{ ts_nodash }}",  # unique id
        wait_for_completion=True,
        poke_interval=1,
        trigger_rule="none_failed",
        conf=pull_dag_params,
        max_active_tis_per_dagrun=512,  # Setting this lower can be useful for debugging
        retries=4,  # Pull can crash; can mean bad luck or the force constant is too high
        retry_delay=timedelta(seconds=0),
    )
    pull_dag_params >> trigger_pull_dag
    pull_info = json_from_dataset_path_parts.override(task_id="pull_results")(
        dataset_path_parts=[
            output_dir,
            f"iteration_{iteration}",
            "{{ task_instance.map_index }}",
            "pull",
            pull_expected_output,
        ],
        key=force_constant,
    )
    trigger_pull_dag >> pull_info

    next_params = new_input(iter_params, pull_info, phi_psi)
    trigger_run_swarm = TriggerDagRunOperator(
        task_id=f"trigger_run_swarm",
        trigger_dag_id="simulate_expand_uniform",
        trigger_run_id="{{ task_instance.map_index }}_{{ task_instance.try_number }}_{{ ts_nodash }}",  # unique id
        wait_for_completion=True,
        poke_interval=1,
        trigger_rule="none_failed",
        conf=next_params["params"],
        max_active_tis_per_dagrun=512,  # Setting this lower can be useful for debugging
        retries=2,
        retry_delay=timedelta(seconds=0),
    )
    sim_info = json_from_dataset_path_parts.override(task_id="extract_sim_info")(
        dataset_path_parts=[
            output_dir,
            f"iteration_{iteration}",
            "{{ task_instance.map_index }}",
            f"{sim_expected_output}.json",
        ],
    )
    # TODO: move this out of this task_group so that it is only called once per step invocation.
    # Currently this task must be executed serially to prevent data write conflicts.
    add_sim_info = add_to_dataset.override(task_id="add_sim_info")(
        output_dir=output_dir,
        output_fn=swarm_expected_output,
        new_data={
            "sims": sim_info,
            "phi_psi": phi_psi,
            "pull": pull_info,
        },
        new_data_keys=[
            f"iteration_{iteration}",
            "{{ task_instance.map_index }}",
        ],
    )
    drift = compute_drift(phi_psi, sim_info, pull_info)
    trigger_run_swarm >> sim_info >> add_sim_info

    return drift


@task
def next_step_params_drift(drift, dag_params, output_dir):
    import logging
    import os

    logging.info(f"drift: {drift}")
    for point, gro in drift.items():
        logging.info(f"point: {point}, gro: {gro}")
    logging.info(f"dag_params: {dag_params}")
    logging.info(f"output_dir: {output_dir}")

    dag_params["iteration"] += 1
    dag_params["output_dir"] = output_dir
    dag_params["inputs"]["point"] = []
    dag_params["inputs"]["gro"]["directory"] = []
    dag_params["inputs"]["gro"]["filename"] = []
    dag_params["inputs"]["gro"]["ref_data"] = False
    for point, gro_path in drift.items():
        gro_path, gro_fn = os.path.split(gro_path)
        dag_params["inputs"]["gro"]["directory"].append(gro_path)
        dag_params["inputs"]["gro"]["filename"].append(gro_fn)
        dag_params["inputs"]["point"].append(point)

    return dag_params


@task
def get_phi_psi_point(iter_params, num_points):
    import numpy as np
    import ast

    # "Coarse Master Equations for Peptide Folding Dynamics" J. Phys. Chem. B, 2008, 112 (19)
    Hummer = [[-50, -50], [-70, 120]]
    # "Finding transition pathways using the string method with swarms of trajectories" J Phys Chem B. 2008 112 (11)
    Pan = [[-82.7, 73.5], [70.5, -69.5]]
    random = np.random.uniform(
        low=[-180, -150], high=[90, 180], size=(num_points, 2)
    ).tolist()

    x_points = np.linspace(-150, 90, 5)
    y_points = np.linspace(-120, 120, 5)
    grid = np.array(np.meshgrid(x_points, y_points)).T.reshape(-1, 2).tolist()
    use = Hummer
    if iter_params["iteration"] == 1:
        if use == random:
            return random
        if use == grid:
            return grid
    points_list = np.linspace(use[0], use[1], num_points).tolist()
    if iter_params["iteration"] == 1:
        return points_list
    else:
        points_list = iter_params["inputs"]["point"]
        points_list = [ast.literal_eval(point) for point in points_list]
        return points_list


@task
def reparametrize(drift, output_dir, iteration, add_point, max_swarms):
    import logging
    import numpy as np
    from sklearn.preprocessing import MinMaxScaler
    import matplotlib.pyplot as plt
    import copy

    logging.info(f"drift: {drift}")

    def string_length(string):
        x = string[:, 0]
        y = string[:, 1]
        dx, dy = np.diff(x), np.diff(y)
        ds = np.array((0, *np.sqrt(dx * dx + dy * dy)))
        return np.sum(ds)

    def interpolate_string(string, num_points):
        x = string[:, 0]
        y = string[:, 1]
        # compute the distances, ds, between points
        dx, dy = np.diff(x), np.diff(y)
        ds = np.array((0, *np.sqrt(dx * dx + dy * dy)))
        # compute the total distance from the 1st point, measured on the curve
        s = np.cumsum(ds)

        # interpolate
        xinter = np.interp(np.linspace(0, s[-1], num_points), s, x)
        yinter = np.interp(np.linspace(0, s[-1], num_points), s, y)
        interpolated_string = np.array([xinter, yinter]).T
        return interpolated_string

    def iter_interpolate_string(string, num_points, max_iterations=5, tol=1e-2):
        prev_string = interpolate_string(string, num_points)
        assert max_iterations > 0
        for i in range(max_iterations):
            new_string = interpolate_string(prev_string, num_points)
            dist = np.linalg.norm(new_string - prev_string) / np.linalg.norm(
                prev_string
            )
            prev_string = new_string
            logging.debug(f"iteration {i}, dist: {dist}")
            if dist <= tol:
                break
        return prev_string

    final_points_string = np.array([list(d["final_point_means"]) for d in drift])
    original_string = np.array([list(d["phi_psi_points"]) for d in drift])
    final_points = np.array([d["final_points"] for d in drift])
    gro_files = [d["best_gro"] for d in drift]
    scaler = MinMaxScaler()
    string = copy.deepcopy(final_points_string)
    # Fix the endpoints
    string[0] = original_string[0]
    string[-1] = original_string[-1]
    add = 0
    if add_point and len(string) < max_swarms:
        add = 1
        # extend gro_files by copying the middle gro file
        middle_gro_idx = len(gro_files) // 2
        gro_files.insert(middle_gro_idx, gro_files[middle_gro_idx])

    interpolate = iter_interpolate_string(string, len(string) + add, 10)
    scaled_string = scaler.fit_transform(interpolate)
    new_string = scaler.inverse_transform(scaled_string)
    new_string_length = np.round(string_length(new_string), 2)
    original_sting_length = np.round(string_length(original_string), 2)
    logging.info(
        f"original length: {original_sting_length}, new length: {new_string_length}"
    )
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    for i, ax in enumerate([ax1, ax2]):
        ax.plot(
            new_string[:, 0],
            new_string[:, 1],
            label="new",
            marker="o",
        )
        ax.plot(
            original_string[:, 0],
            original_string[:, 1],
            label="original",
            marker="o",
        )
        ax.legend(loc="best")
        ax.grid(True)
        ax.set_xlabel("Psi")
        ax.set_ylabel("Phi")
    ax1.scatter(
        final_points[:, :, 0], final_points[:, :, 1], label="final points", marker="x"
    )
    ax1.set_title(f"Detail view")
    ax2.set_xlim(-180, 180)
    ax2.set_ylim(-180, 180)
    ax2.set_xticks([-180, -90, 0, 90, 180])
    ax2.set_yticks([-180, -90, 0, 90, 180])
    ax2.set_title(f"Full view")
    plt.suptitle(f"Reparametrized string, iteration {iteration}")
    plt.savefig(f"{output_dir}/reparametrized_string.png")
    plt.close()

    new_string_dict = {}
    for i in range(len(new_string)):
        new_string_dict[str(np.round(new_string[i]).tolist())] = gro_files[i]
    return new_string_dict


@task
def compute_convergence(drift):
    import logging
    import numpy as np

    initial_points = [d["initial_point_means"] for d in drift]
    initial_points = np.array(initial_points)
    final_points = [d["final_point_means"] for d in drift]
    final_points = np.array(final_points)
    points = [d["phi_psi_points"] for d in drift]
    points = np.array(points)

    def vector_angle(u, v):
        u = np.array(u)
        v = np.array(v)
        cos_theta = np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))
        angle_rad = np.arccos(np.clip(cos_theta, -1.0, 1.0))
        angle_deg = np.degrees(angle_rad)
        return angle_deg

    swarm_avg_vectors = {}
    for i, point in enumerate(points):
        logging.debug(f"point: {point}")
        logging.debug(f"initial: {initial_points[i]}")
        logging.debug(f"final: {final_points[i]}")
        swarm_avg_vectors[str(point)] = [initial_points[i], final_points[i]]

    points_T = np.transpose(points)
    x_range = np.max(points_T[0]) - np.min(points_T[0])
    y_range = np.max(points_T[1]) - np.min(points_T[1])
    long_axis = 0 if x_range > y_range else 1
    logging.info(f"Will sort points based on axis {'x' if long_axis == 0 else 'y'}")
    sorted_points = sorted(points, key=lambda x: x[long_axis])
    logging.info(f"sorted points: {[pt.tolist() for pt in sorted_points]}")

    angle_errors = []
    for i, point in enumerate(sorted_points):
        swarm_avg = swarm_avg_vectors[str(point)]
        swarm_vec = np.array(swarm_avg[-1]) - np.array(swarm_avg[0])
        angle_fwd, angle_rev = 180, 180
        if i > 0:
            point_rev = np.array(sorted_points[i - 1]) - np.array(sorted_points[i])
            angle_rev = vector_angle(swarm_vec, point_rev)
            logging.debug(f"point-1: {sorted_points[i - 1]}")
        logging.info(f"point: {point}")
        if i < len(sorted_points) - 1:
            point_fwd = np.array(sorted_points[i + 1]) - np.array(sorted_points[i])
            angle_fwd = vector_angle(swarm_vec, point_fwd)
            logging.debug(f"point+1: {sorted_points[i + 1]}")
        logging.info(
            f"fwd angle: {round(angle_fwd, 2)}, rev angle: {round(angle_rev, 2)}"
        )
        angle_errors.append(np.round(min(angle_fwd, angle_rev), 2).tolist())
    logging.info(f"angle errors: {angle_errors}")
    cumulative_error = np.round(np.mean(angle_errors), 2)
    logging.info(f"cumulative error: {cumulative_error}")
    return cumulative_error


@task
def swarms_done(iteration, max_iterations, convergence_threshold, convergence):
    return iteration >= max_iterations or convergence < convergence_threshold


with DAG(
    dag_id="swarms",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
                "gro": {
                    "directory": "ala_pentapeptide",
                    "filename": "ala_penta_capped_solv.gro",
                    "ref_data": True,
                },
                "top": {
                    "directory": "ala_pentapeptide",
                    "filename": "ala_penta_capped_solv.top",
                    "ref_data": True,
                },
            },
            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
            },
            section="inputs",
        ),
        "output_dir": "swarms",
        "mdp_options": Param(
            {
                "swarm": {"nsteps": 2500, "nstxout-compressed": 500},
                "pull": {
                    "nsteps": 5000,
                    "nstxout-compressed": 1000,
                    "dt": 0.001,
                },
            },
            type=["object", "null"],
            title="MDP options",
            section="mdp_options",
        ),
        "pull_force_constant": 350,
        "convergence_threshold": 5,
        "output_files": Param(
            {
                "swarm": "swarms.json",
                "sim": "result",
                "pull": "dihedrals.json",
            },
            type=["object", "null"],
            title="Output files",
            section="output_files",
        ),
        "max_iterations": 2,
        "swarm_size": 4,
        "num_string_points": 4,
        "max_string_points": 4,
    },
) as swarms:
    prev_results = json_from_dataset_path.override(task_id="get_iteration_params")(
        dataset_path="{{ params.output_dir }}/{{ params.output_files.swarm }}",
        allow_missing=True,
    )
    num_completed_iters = iterations_completed(
        prev_results, "{{ params.max_iterations }}"
    )
    this_iter_params = iteration_params(
        completed_iterations=num_completed_iters, previous_results=prev_results
    )
    add_first_params = add_to_dataset.override(task_id="add_first_params")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.output_files.swarm }}",
        new_data=this_iter_params["params"],
        new_data_keys=["iteration_1", "params"],
    )
    do_first_params_add = short_circuit.override(task_id="skip_add_first_params")(
        this_iter_params["is_first_iteration"]
    )
    do_first_params_add >> add_first_params
    do_this_iteration = short_circuit.override(task_id="skip_this_iteration")(
        this_iter_params["trigger_this_iteration"]
    )

    phi_psi_point = get_phi_psi_point(
        this_iter_params["params"], "{{ params.num_string_points }}"
    )
    drift = step.partial(
        output_dir="{{ params.output_dir }}",
        iter_params=this_iter_params["params"],
        iteration=this_iter_params["iteration"],
        force_constant="{{ params.pull_force_constant }}",
        swarm_expected_output="{{ params.output_files.swarm }}",
        sim_expected_output="{{ params.output_files.sim }}",
        pull_expected_output="{{ params.output_files.pull }}",
    ).expand(phi_psi=phi_psi_point)

    do_this_iteration >> drift
    next_string = reparametrize(
        drift=drift,
        output_dir=this_iter_params["output_dir"],
        iteration="{{ task_instance.xcom_pull(task_ids='iteration_params', key='iteration') }}",
        add_point=False,
        max_swarms="{{ params.max_string_points }}",
    )
    convergence = compute_convergence(drift)
    add_convergence_info = add_to_dataset.override(task_id="add_convergence_info")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.output_files.swarm }}",
        new_data=convergence,
        new_data_keys=[
            "iteration_{{ task_instance.xcom_pull(task_ids='iteration_params', key='iteration') }}",
            "convergence",
        ],
    )
    next_params = next_step_params_drift(
        next_string, this_iter_params["params"], "{{ params.output_dir }}"
    )
    add_drift_info = add_to_dataset.override(task_id="add_drift_info")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.output_files.swarm }}",
        new_data=next_params,
        new_data_keys=[
            "iteration_{{ task_instance.xcom_pull(task_ids='iteration_params', key='iteration')  + 1 }}",
            "params",
        ],
    )

    do_next_iteration = swarms_done(
        "{{ task_instance.xcom_pull(task_ids='iteration_params', key='iteration') }}",
        "{{ params.max_iterations }}",
        "{{ params.convergence_threshold }}",
        convergence,
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="swarms",
        dag_params=next_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )
    next_params >> do_next_iteration >> next_iteration
