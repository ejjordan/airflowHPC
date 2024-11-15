from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import (
    run_if_false,
    verify_files,
    json_from_dataset_path,
    evaluate_template_truth,
)


@task(multiple_outputs=True)
def atoms_distance(
    gro, atom_sel1: str = "name CA and resid 1", atom_sel2: str = "name CA and resid 5"
):
    import MDAnalysis as mda
    from MDAnalysis.analysis import distances

    u = mda.Universe(gro)
    atom1 = u.select_atoms(atom_sel1)
    atom2 = u.select_atoms(atom_sel2)
    _, _, distance = distances.dist(atom1, atom2)
    assert len(distance) == 1
    return {"distance": distance[0], "gro": gro}


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
def next_step_params(gro, dag_params):
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


with DAG(
    dag_id="swarms",
    start_date=timezone.utcnow(),
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
            {"nsteps": 5000},
            {"nsteps": 5000},
            {"nsteps": 5000},
            {"nsteps": 5000},
        ],
        "iteration": 1,
        "max_iterations": 3,
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
            "gro": "-c",
        },
    }
    swarm_has_run = verify_files.override(task_id="swarm_has_run")(
        input_dir="{{ params.output_dir }}/iteration_{{ params.iteration }}",
        filename="result.gro",
        mdp_options="{{ params.mdp_options }}",
    )
    run_swarm = run_if_false.override(group_id="run_swarm")(
        dag_id="simulate_expand",
        dag_params=swarm_params,
        truth_value=swarm_has_run,
        dag_display_name="run_swarm",
    )
    sim_info = json_from_dataset_path.override(task_id="extract_sim_info")(
        dataset_path="{{ params.output_dir }}/iteration_{{ params.iteration }}/result.json",
        key="gro",
    )
    distances = atoms_distance.expand(gro=sim_info)
    run_swarm >> sim_info

    distance_data = distances.map(lambda x: x)
    next_gro = next_step_gro(distance_data)
    next_iteration_params = next_step_params(next_gro["min"], "{{ params }}")

    do_next_iteration = evaluate_template_truth.override(
        task_id="do_next_iteration", trigger_rule="none_failed_min_one_success"
    )(
        statement="{{ params.iteration }} >= {{ params.max_iterations }}",
    )
    next_iteration = run_if_false.override(group_id="next_iteration")(
        dag_id="swarms",
        dag_params=next_iteration_params,
        truth_value=do_next_iteration,
        wait_for_completion=False,
    )
