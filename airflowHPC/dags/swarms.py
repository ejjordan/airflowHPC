from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.exceptions import AirflowException

from airflowHPC.dags.tasks import (
    run_if_false,
    json_from_dataset_path,
    evaluate_template_truth,
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
def ramachandran_analysis(
    inputs, resnums, output_dir, list_of_points=[[-50, -50], [120, -70]]
):
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
    for point in list_of_points:
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

    distance = gro["distance"]
    if dag_params["best_gro"] is None or distance < dag_params["best_gro"]["distance"]:
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
def next_step_params_rama(rama_output, dag_params):
    import os

    for point, items in rama_output.items():
        gro = items["fn"]
        distance = items["distance"]
        if (
            dag_params["best_gro"] is None
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
    return dag_params


@task
def add_to_dataset(
    output_dir: str, output_fn: str, new_data: dict, new_data_keys: list[str]
):
    import os
    import json

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, output_fn)
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            data = json.load(f)
    else:
        data = {}

    # Navigate through the nested keys
    nested_data = data
    for key in new_data_keys[:-1]:
        if key not in nested_data:
            nested_data[key] = {}
        nested_data = nested_data[key]

    # Assign the new data to the final key
    nested_data[new_data_keys[-1]] = new_data

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2, separators=(",", ": "))
    dataset = Dataset(uri=output_file)
    return dataset


@task(multiple_outputs=True)
def iterations_completed(dataset_dict, max_iterations, this_iteration):
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

    return {
        "highest_completed_iteration": highest_completed_iteration,
        "this_iteration_done": this_iteration <= highest_completed_iteration,
    }


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
                "filename": "ala_penta_solv.gro",
                "ref_data": True,
            },
            "top": {
                "directory": "ala_pentapeptide",
                "filename": "ala_penta_solv.top",
                "ref_data": True,
            },
        },
        "output_dir": "swarms",
        "mdp_options": [
            {"nsteps": 500, "nstxout-compressed": 100},
            {"nsteps": 500, "nstxout-compressed": 100},
            {"nsteps": 500, "nstxout-compressed": 100},
        ],
        "iteration": 1,
        "max_iterations": 2,
        "best_gro": None,
    },
) as swarms:
    swarm_params = {
        "inputs": {
            "mdp": "{{ params.inputs.mdp }}",
            "gro": "{{ params.inputs.gro }}",
            "top": "{{ params.inputs.top }}",
        },
        "mdp_options": "{{ params.mdp_options }}",
        "output_dir": "{{ params.output_dir }}/iteration_{{ params.iteration }}",
        "expected_output": "result.gro",
        "output_dataset_structure": {
            "simulation_id": "simulation_id",
            "gro": "-c",
            "tpr": "-s",
            "xtc": "-x",
        },
    }
    prev_results = json_from_dataset_path.override(task_id="get_iteration_params")(
        dataset_path="{{ params.output_dir }}/swarms.json", allow_missing=True
    )
    num_completed_iters = iterations_completed(
        prev_results, "{{ params.max_iterations }}", "{{ params.iteration }}"
    )
    run_swarm = run_if_false.override(group_id="run_swarm")(
        dag_id="simulate_expand",
        dag_params=swarm_params,
        truth_value=num_completed_iters["this_iteration_done"],
        dag_display_name="run_swarm",
    )
    sim_info = json_from_dataset_path.override(task_id="extract_sim_info")(
        dataset_path="{{ params.output_dir }}/iteration_{{ params.iteration }}/result.json",
    )
    add_sim_info = add_to_dataset.override(task_id="add_sim_info")(
        output_dir="{{ params.output_dir }}",
        output_fn="swarms.json",
        new_data=sim_info,
        new_data_keys=["iteration_{{ params.iteration }}", "sims"],
    )
    run_swarm >> sim_info >> add_sim_info

    # distances = atoms_distance.expand(inputs=sim_info)
    # distance_data = distances.map(lambda x: x)
    # next_dist_gro = next_step_gro(distance_data)
    # next_dist_params = next_step_params_dist(next_dist_gro["min"], "{{ params }}")

    rama = ramachandran_analysis(
        inputs=sim_info,
        resnums="1-5",
        output_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        list_of_points=[[-50, -50]],
    )
    next_rama_params = next_step_params_rama(rama, "{{ params }}")
    add_rama_info = add_to_dataset.override(task_id="add_rama_info")(
        output_dir="{{ params.output_dir }}",
        output_fn="swarms.json",
        new_data=next_rama_params,
        new_data_keys=["iteration_{{ params.iteration + 1 }}", "params"],
    )

    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed_min_one_success"
    )(
        statement="{{ params.iteration }} >= {{ params.max_iterations }}",
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="swarms",
        dag_params=next_rama_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )
    add_rama_info >> do_next_iteration >> next_iteration
