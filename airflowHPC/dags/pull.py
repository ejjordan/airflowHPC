from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.decorators import task


@task
def make_ndx_dihedrals(gro, output_dir, output_fn):
    from MDAnalysis import Universe
    import logging, os

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    u = Universe(gro)
    u.atoms.write(f"{output_dir}/{output_fn}", name="system")
    protein = u.select_atoms("protein")
    ndx_dict = {}
    phi_psi_dict = {}
    for res in protein.residues:
        logging.info(f"Processing residue {res.resid} with name {res.resname}")
        if res.resname != "ALA":
            continue
        phi = res.phi_selection()
        logging.info(f"Phi selection: {phi}")
        phi_psi_dict[str(res.resid)] = {"phi": [], "psi": []}
        for atom in phi.atoms:
            name = f"{atom.name}_r{atom.resid}"
            ndx_dict[name] = atom.id
            phi_psi_dict[str(res.resid)]["phi"].append({name: int(atom.id)})
            logging.info(
                f"Phi atom: {name}, index: {atom.id}, size: {len(ndx_dict.items())}"
            )
        psi = res.psi_selection()
        logging.info(f"Psi selection: {psi}")
        for atom in psi.atoms:
            name = f"{atom.name}_r{atom.resid}"
            ndx_dict[name] = atom.id
            phi_psi_dict[str(res.resid)]["psi"].append({name: int(atom.id)})
            logging.info(
                f"Psi atom: {name}, index: {atom.id}, size: {len(ndx_dict.items())}"
            )
    logging.info(f"ndx_dict: {ndx_dict}")
    with open(f"{output_dir}/{output_fn}", "a") as f:
        for name, idx in ndx_dict.items():
            f.write(f"[ {name} ]\n")
            f.write(f"   {idx}\n")
    return phi_psi_dict


@task
def mdp_update_params(phi_psi_dict, extra_updates):
    mdp_updates = {}
    group_name_index = 1
    pull_group_index = 1
    for res, phi_psi in phi_psi_dict.items():
        group_idxs = []
        for phi in phi_psi["phi"]:
            for name, idx in phi.items():
                mdp_updates[f"pull-group{group_name_index}-name"] = name
                group_idxs.append(group_name_index)
                group_name_index += 1
        group_string = f"{group_idxs[0]} {group_idxs[1]} {group_idxs[1]} {group_idxs[2]} {group_idxs[2]} {group_idxs[3]}"
        mdp_updates[f"pull-coord{pull_group_index}-groups"] = group_string
        mdp_updates[f"pull-coord{pull_group_index}-k"] = 100
        mdp_updates[f"pull-coord{pull_group_index}-rate"] = 0.001
        mdp_updates[f"pull-coord{pull_group_index}-init"] = -70
        mdp_updates[f"pull-coord{pull_group_index}-geometry"] = "dihedral"
        pull_group_index += 1
    mdp_updates["pull-ngroups"] = group_name_index - 1
    mdp_updates["pull-ncoords"] = pull_group_index - 1
    mdp_updates["pull"] = "yes"
    mdp_updates.update(extra_updates)
    return mdp_updates


@task
def calculate_dihedrals(init_gro, final_gro):
    import logging
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran

    for gro, title in [(init_gro, "Initial"), (final_gro, "Final")]:
        u = Universe(gro)
        protein = u.select_atoms("protein")
        rama = Ramachandran(protein).run()
        phi_psi_angles = rama.results.angles
        means = phi_psi_angles.mean(axis=1)
        logging.info(f"{title} Phi/Psi angle means: {means}")
        logging.info(f"{title} Phi/Psi angles: {phi_psi_angles}")


with DAG(
    "pull",
    start_date=timezone.utcnow(),
    catchup=False,
    params={
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "sim.json"},
            "gro": {
                "directory": "ala_pentapeptide",
                "filename": "ala_penta_capped_solv.gro",
            },
            "top": {
                "directory": "ala_pentapeptide",
                "filename": "ala_penta_capped_solv.top",
            },
        },
        "output_dir": "pulling",
        "index_fn": "dihedrals.ndx",
    },
) as dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
    )
    dih_ndx = make_ndx_dihedrals(
        gro=input_gro,
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.index_fn }}",
    )
    mdp_updates = mdp_update_params(dih_ndx, extra_updates={"nsteps": 500})
    input_top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update")(
        mdp_json_file_path=input_mdp,
        update_dict=mdp_updates,
    )

    grompp_result = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={
            "-f": mdp,
            "-c": input_gro,
            "-p": input_top,
            "-n": "{{ params.index_fn }}",
        },
        output_files={"-o": "run.tpr"},
        output_dir="{{ params.output_dir }}",
    )

    mdrun_result = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir="{{ params.output_dir }}",
    )
    final_dihedrals = calculate_dihedrals.override(task_id="dihedrals_values")(
        init_gro=input_gro, final_gro="{{ ti.xcom_pull(task_ids='mdrun')['-c'] }}"
    )
    grompp_result >> mdrun_result >> final_dihedrals
