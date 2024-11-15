from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import run_if_false, verify_files, json_from_dataset_path


@task
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
    return distance[0]


with DAG(
    dag_id="swarms",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "output_dir": "swarms",
        "mdp_options": [{"nsteps": 5000}, {"nsteps": 5000}, {"nsteps": 5000}],
    },
) as swarms:
    swarm_params = {
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
        "mdp_options": "{{ params.mdp_options }}",
        "output_dir": "{{ params.output_dir }}",
        "expected_output": "result.gro",
        "output_dataset_structure": {
            "simulation_id": "simulation_id",
            "gro": "-c",
        },
    }
    swarm_has_run = verify_files.override(task_id="swarm_has_run")(
        input_dir="{{ params.output_dir }}",
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
        dataset_path="{{ params.output_dir }}/result.json",
        key="gro",
    )
    distances = atoms_distance.expand(gro=sim_info)
    run_swarm >> sim_info
