from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflowHPC.dags.tasks import (
    json_from_dataset_path,
    add_to_dataset,
)


@task(multiple_outputs=True)
def atoms_distance(
    inputs,
    atom_sel1: str = "name CA and resid 1",
    atom_sel2: str = "name CA and resid 5",
):
    from MDAnalysis import Universe
    from MDAnalysis.analysis import distances

    gro = inputs["gro"]
    u = Universe(gro)
    atom1 = u.select_atoms(atom_sel1)
    atom2 = u.select_atoms(atom_sel2)
    _, _, distance = distances.dist(atom1, atom2)
    assert len(distance) == 1
    return {"distance": distance[0], "gro": gro}


# From "Coarse Master Equations for Peptide Folding Dynamics" J. Phys. Chem. B, 2008, 112 (19)
@task(trigger_rule="none_failed", multiple_outputs=True)
def ramachandran_analysis(inputs, resnums, output_dir, phi_psi_point=[-50, -50]):
    import logging
    from math import dist
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran
    from MDAnalysis import Writer

    # MDAnalysis is too verbose
    logging.getLogger("MDAnalysis").setLevel(logging.WARNING)
    frames_means = []
    for item in inputs:
        xtc = item["xtc"]
        tpr = item["tpr"]
        u = Universe(tpr, xtc)
        selected_residues = u.select_atoms(f"resid {resnums}")
        rama = Ramachandran(selected_residues).run()
        phi_psi_frame_means = rama.results.angles.mean(axis=1)
        frames_means.append(
            {"means": phi_psi_frame_means.tolist(), "xtc": xtc, "tpr": tpr}
        )

    best_frames = {}
    point = phi_psi_point
    str_point = str(point)
    best_frames[str_point] = {}
    for frame in frames_means:
        nearest_frame = min(frame["means"], key=lambda x: dist(x, point))
        nearest_frame_idx = frame["means"].index(nearest_frame)
        if best_frames[str_point] == {} or dist(nearest_frame, point) < dist(
            best_frames[str_point]["means"], point
        ):
            best_frames[str_point]["means"] = nearest_frame
            best_frames[str_point]["distance"] = dist(nearest_frame, point)
            best_frames[str_point]["frame_idx"] = nearest_frame_idx
            best_frames[str_point]["xtc"] = frame["xtc"]
            best_frames[str_point]["tpr"] = frame["tpr"]
    msg = f"Closest point to {point} is {[round(pt, 3) for pt in best_frames[str_point]['means']]}, "
    msg += f"which is {round(best_frames[str_point]['distance'], 3)} away "
    msg += f"from timestep {best_frames[str_point]['frame_idx']} in {best_frames[str_point]['xtc']}"
    logging.info(msg)

    point_output = {}
    for i, (point, data) in enumerate(best_frames.items()):
        universe = Universe(data["tpr"], data["xtc"])
        output_fn = f"{output_dir}/best_frame{i}.gro"
        with Writer(output_fn, universe.atoms.n_atoms) as W:
            universe.trajectory[data["frame_idx"]]
            W.write(universe.atoms)
        point_output[point] = {"fn": output_fn, "distance": data["distance"]}

    return point_output


@task(multiple_outputs=True)
def next_step_gro(distance_info):
    import logging

    distance_info = [info for info in distance_info]
    for info in distance_info:
        logging.info(f"Distance: {info['distance']} in {info['gro']}")
    min_dist = min(distance_info, key=lambda x: x["distance"])
    max_dist = max(distance_info, key=lambda x: x["distance"])
    return {"min": min_dist, "max": max_dist}


@task
def next_step_params_dist(gro, dag_params):
    import os

    if "best_gro" not in dag_params:
        dag_params["best_gro"] = {"gro": None, "distance": None}
    distance = gro["distance"]
    if (
        dag_params["best_gro"]["distance"] is None
        or distance < dag_params["best_gro"]["distance"]
    ):
        dag_params["best_gro"] = gro
        next_gro = gro["gro"]
    else:
        next_gro = dag_params["best_gro"]["gro"]

    gro_path, gro_fn = os.path.split(next_gro)
    dag_params["inputs"]["gro"]["directory"] = gro_path
    dag_params["inputs"]["gro"]["filename"] = gro_fn
    dag_params["iteration"] += 1
    return dag_params


@task
def next_step_params_rama(rama_output, dag_params, output_dir):
    import os

    if "best_gro" not in dag_params:
        dag_params["best_gro"] = {"gro": None, "distance": None}
    for point, items in rama_output.items():
        gro = items["fn"]
        distance = items["distance"]
        if (
            dag_params["best_gro"]["distance"] is None
            or distance < dag_params["best_gro"]["distance"]
        ):
            dag_params["best_gro"] = {"gro": gro, "distance": distance}
            next_gro = gro
        else:
            next_gro = dag_params["best_gro"]["gro"]
        gro_path, gro_fn = os.path.split(next_gro)
        dag_params["inputs"]["gro"]["directory"] = gro_path
        dag_params["inputs"]["gro"]["filename"] = gro_fn
        dag_params["inputs"]["gro"]["ref_data"] = False
    dag_params["iteration"] += 1
    dag_params["output_dir"] = output_dir
    return dag_params


@task
def iterations_completed(dataset_dict, max_iterations):
    import logging
    import os

    highest_completed_iteration = 0
    for iteration in range(1, max_iterations + 1):
        iteration_name = f"iteration_{iteration}"
        if iteration_name in dataset_dict:
            iteration_data = dataset_dict[iteration_name]
            logging.info(f"iteration_data: {iteration_data}")
            if "sims" in iteration_data:
                sims = iteration_data["sims"]
                for sim in sims:
                    if "gro" in sim:
                        if "simulation_id" not in sim:
                            logging.error(f"No simulation_id in {sim}.")
                            raise AirflowException(f"No simulation_id in {sim}.")
                        if not os.path.exists(sim["gro"]):
                            logging.error(f"Missing gro file: {sim['gro']}")
                            raise AirflowException(f"File {sim['gro']} not found.")
                        logging.info(
                            f"Iteration {iteration}, simulation {sim['simulation_id']} has gro file."
                        )
                    else:
                        logging.info(
                            f"No gro file in iteration {iteration}, simulation {sim['simulation_id']}"
                        )
                        break
                highest_completed_iteration = iteration
                logging.info(f"Iteration {iteration} has all gro files.")
            else:
                logging.info(f"No sims in iteration {iteration}")
                break
        else:
            logging.info(f"No data for iteration {iteration}")
            break

    return highest_completed_iteration


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
        dag_params["best_gro"] = {"gro": None, "distance": None}
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
    return {"params": dag_params, "output_dir": new_output_dir}


@task
def pull_params(
    phi_psi,
    output_path,
    iter_params,
    iteration,
    force_constant,
    expected_output="dihedrals.json",
    **context,
):
    import os
    import logging

    logging.info(f"phi_psi: {phi_psi}")
    phi = phi_psi[0]
    psi = phi_psi[1]
    ti = context["ti"]
    map_index = ti.map_index
    output_dir = f"{output_path}/iteration_{iteration}/{map_index}"
    if "best_gro" in iter_params and iter_params["best_gro"]["gro"] is not None:
        new_gro = iter_params["best_gro"]["gro"]
        gro_path, gro_fn = os.path.split(new_gro)
        ref_data = False
    else:
        gro_path = iter_params["inputs"]["gro"]["directory"]
        gro_fn = iter_params["inputs"]["gro"]["filename"]
        ref_data = True
    gro = {"directory": gro_path, "filename": gro_fn, "ref_data": ref_data}
    return {
        "phi_angle": phi,
        "psi_angle": psi,
        "force_constant": force_constant,
        "output_dir": output_dir,
        "expected_output": expected_output,
        "gro": gro,
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
    last_frames = []
    last_frames_means = []
    for sim_data in sim_info:
        tpr = sim_data["tpr"]
        xtc = sim_data["xtc"]
        u = Universe(tpr, xtc)
        selected_residues = u.select_atoms("protein")
        rama = Ramachandran(selected_residues).run()
        first = rama.results.angles[0]
        logging.info(f"num angles: {len(first)}")
        f_mean = np.mean(first, axis=0)
        f_stdev = np.std(first, axis=0)
        logging.info(
            f"frame first mean: {np.round(f_mean, 4)}, first stdev: {np.round(f_stdev, 4)}"
        )
        last = rama.results.angles[-1]
        l_mean = np.mean(last, axis=0)
        l_stdev = np.std(last, axis=0)
        logging.info(
            f"frame last mean: {np.round(l_mean, 4)}, last stdev: {np.round(l_stdev, 4)}"
        )
        last_frames_means.append(l_mean.tolist())
        last_frames.append(rama.results.angles[-1])

    last = np.concatenate(last_frames, axis=0)
    last_mean = last.mean(axis=0)
    last_stdev = last.std(axis=0)
    logging.info(f"cumulative last mean: {last_mean}, last stdev: {last_stdev}")

    # find the last frame closest to last mean
    closest_sim_idx = np.argmin(
        [np.linalg.norm(x - last_mean) for x in np.array(last_frames_means)]
    )
    best_gro = sim_info[closest_sim_idx]["gro"]
    logging.info(f"best_gro: {best_gro}, type: {type(best_gro)}")

    return {
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
    force_constant="200",
    expected_output="dihedrals.json",
):
    pull_dag_params = pull_params(
        phi_psi, output_dir, iter_params, iteration, force_constant, expected_output
    )
    trigger_pull_dag = TriggerDagRunOperator(
        task_id="trigger_pull_dag",
        trigger_dag_id="pull",
        wait_for_completion=True,
        poke_interval=1,
        trigger_rule="none_failed",
        conf=pull_dag_params,
    )
    pull_dag_params >> trigger_pull_dag
    pull_info = json_from_dataset_path_parts.override(task_id="pull_results")(
        dataset_path_parts=[
            output_dir,
            f"iteration_{iteration}",
            "{{ task_instance.map_index }}",
            expected_output,
        ],
        key=force_constant,
    )
    trigger_pull_dag >> pull_info

    next_params = new_input(iter_params, pull_info, phi_psi)
    trigger_run_swarm = TriggerDagRunOperator(
        task_id=f"trigger_run_swarm",
        trigger_dag_id="simulate_expand",
        wait_for_completion=True,
        poke_interval=1,
        trigger_rule="none_failed",
        conf=next_params["params"],
    )
    sim_info = json_from_dataset_path_parts.override(task_id="extract_sim_info")(
        dataset_path_parts=[
            output_dir,
            f"iteration_{iteration}",
            "{{ task_instance.map_index }}",
            "result.json",
        ],
    )
    add_sim_info = add_to_dataset.override(task_id="add_sim_info")(
        output_dir=output_dir,
        output_fn="swarms.json",
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

    return drift, add_sim_info


@task
def new_string(drift, output_dir):
    import logging
    import numpy as np
    import matplotlib.pyplot as plt

    logging.info(f"drift: {drift}")

    last_point_means = [np.round(item["final_point_means"]).tolist() for item in drift]
    last_points = [item["final_points"] for item in drift]
    phi_psi_points = [item["phi_psi_points"] for item in drift]
    num_points = len(phi_psi_points)

    cmap = plt.cm.get_cmap("rainbow")
    assert len(last_points) == len(phi_psi_points)
    colors = cmap(np.linspace(0, 1, len(phi_psi_points)))
    for i in range(num_points):
        logging.info(f"phi_psi_points: {phi_psi_points[i]}")
        plt.scatter(
            phi_psi_points[i][0],
            phi_psi_points[i][1],
            label=f"Phi/Psi point {i + 1}",
            color=colors[i],
            marker="x",
        )
        logging.info(f"last_point_means: {last_point_means[i]}")
        plt.scatter(
            last_point_means[i][0], last_point_means[i][1], color=colors[i], marker="2"
        )
        for j in range(len(last_points[i])):
            logging.info(f"last_points: {last_points[i][j]}")
            plt.scatter(
                last_points[i][j][0], last_points[i][j][1], color=colors[i], marker="+"
            )

    plt.legend()
    plt.xlim(-180, 180)
    plt.ylim(-180, 180)
    plt.xlabel("Psi")
    plt.ylabel("Phi")
    plt.savefig(f"{output_dir}/old_and_new_paths.png")
    plt.close()

    return last_point_means


@task
def get_phi_psi_point(iter_params, num_points):
    from numpy import linspace

    Hummer = [[-50, -50], [-70, 120]]
    Delamotte = [[-85, 75], [70, -70]]
    use = Hummer
    points_list = linspace(use[0], use[1], num_points).tolist()
    if iter_params["iteration"] == 1:
        return points_list
    else:
        return iter_params["phi_psi_points"]


with DAG(
    dag_id="swarms",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": {
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
        "output_dir": "swarms",
        "mdp_options": [{"nsteps": 100, "nstxout-compressed": 10} for _ in range(3)],
        "expected_output": "result.gro",
        "max_iterations": 2,
    },
) as swarms:
    prev_results = json_from_dataset_path.override(task_id="get_iteration_params")(
        dataset_path="{{ params.output_dir }}/swarms.json", allow_missing=True
    )
    num_completed_iters = iterations_completed(
        prev_results, "{{ params.max_iterations }}"
    )
    this_iter_params = iteration_params(
        completed_iterations=num_completed_iters, previous_results=prev_results
    )
    add_first_params = add_to_dataset.override(task_id="add_first_params")(
        output_dir="{{ params.output_dir }}",
        output_fn="swarms.json",
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

    phi_psi_point = get_phi_psi_point(this_iter_params["params"], 4)
    drift, sim_info = step.partial(
        output_dir="{{ params.output_dir }}",
        iter_params=this_iter_params["params"],
        iteration=this_iter_params["iteration"],
    ).expand(phi_psi=phi_psi_point)

    do_this_iteration >> [drift, sim_info]
    new_string(drift, this_iter_params["output_dir"])

    # distances = atoms_distance.expand(inputs=sim_info)
    # distance_data = distances.map(lambda x: x)
    # next_dist_gro = next_step_gro(distance_data)
    # next_dist_params = next_step_params_dist(next_dist_gro["min"], "{{ params }}")

    """
    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed_min_one_success"
    )(
        statement="{{ task_instance.xcom_pull(task_ids='iteration_params', key='iteration') }} >= {{ params.max_iterations }}",
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="swarms",
        dag_params=next_rama_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )
    next_rama_params >> do_next_iteration >> next_iteration
    """
