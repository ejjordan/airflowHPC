from airflow import DAG
from airflow.utils.timezone import datetime
from airflowHPC.dags.tasks import (
    get_file,
    add_to_dataset,
)
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
def mdp_update_params(
    phi_psi_dict, force_constant, phi_angle, psi_angle, extra_updates, pull_rate=None
):
    import ast

    mdp_updates = {}
    group_name_index = 1
    pull_group_index = 1
    pull_group_names_lookup = {}
    for res, phi_psi in phi_psi_dict.items():
        group_idxs_phi = []
        for phi in phi_psi["phi"]:
            for name, idx in phi.items():
                mdp_updates[f"pull-group{group_name_index}-name"] = name
                group_idxs_phi.append(group_name_index)
                pull_group_names_lookup[name] = group_name_index
                group_name_index += 1
        group_string = f"{group_idxs_phi[0]} {group_idxs_phi[1]} {group_idxs_phi[1]} {group_idxs_phi[2]} {group_idxs_phi[2]} {group_idxs_phi[3]}"
        mdp_updates[f"pull-coord{pull_group_index}-groups"] = group_string
        mdp_updates[f"pull-coord{pull_group_index}-k"] = force_constant
        mdp_updates[f"pull-coord{pull_group_index}-geometry"] = "dihedral"
        if pull_rate and not phi_angle:
            mdp_updates[f"pull-coord{pull_group_index}-start"] = "yes"
            mdp_updates[f"pull-coord{pull_group_index}-rate"] = pull_rate
        elif phi_angle and not pull_rate:
            mdp_updates[f"pull-coord{pull_group_index}-init"] = phi_angle
        else:
            raise ValueError("Either phi_angle or pull_rate must be set, but not both")
        pull_group_index += 1

        group_idxs_psi = []
        for psi in phi_psi["psi"]:
            for name, idx in psi.items():
                if name in pull_group_names_lookup:
                    group_idxs_psi.append(pull_group_names_lookup[name])
                else:
                    mdp_updates[f"pull-group{group_name_index}-name"] = name
                    group_idxs_psi.append(group_name_index)
                    group_name_index += 1
        group_string = f"{group_idxs_psi[0]} {group_idxs_psi[1]} {group_idxs_psi[1]} {group_idxs_psi[2]} {group_idxs_psi[2]} {group_idxs_psi[3]}"
        mdp_updates[f"pull-coord{pull_group_index}-groups"] = group_string
        mdp_updates[f"pull-coord{pull_group_index}-k"] = force_constant
        mdp_updates[f"pull-coord{pull_group_index}-geometry"] = "dihedral"
        if pull_rate and not psi_angle:
            mdp_updates[f"pull-coord{pull_group_index}-start"] = "yes"
            mdp_updates[f"pull-coord{pull_group_index}-rate"] = pull_rate
        elif psi_angle and not pull_rate:
            mdp_updates[f"pull-coord{pull_group_index}-init"] = psi_angle
        else:
            raise ValueError("Either psi_angle or pull_rate must be set, but not both")
        pull_group_index += 1

    mdp_updates["pull-ngroups"] = group_name_index - 1
    mdp_updates["pull-ncoords"] = pull_group_index - 1
    mdp_updates["pull"] = "yes"
    mdp_updates.update(ast.literal_eval(extra_updates))
    return mdp_updates


def plot_timeseries(phi_psi_angles, phi, psi, output_dir):
    import matplotlib.pyplot as plt

    phi = float(phi)
    psi = float(psi)
    # Plot phi/psi angles as a time series
    fig, ax = plt.subplots(nrows=6, ncols=2)
    ax[0, 0].set_title(f"Phi={phi:.0f}")
    ax[0, 1].set_title(f"Psi={psi:.0f}")
    for i, angle in enumerate([phi, psi]):
        ax[0, i].plot(
            phi_psi_angles.mean(axis=1).T[i],
            label=f"Final mean: {phi_psi_angles.mean(axis=1).T[i][-1]:.0f}",
        )
        ax[0, i].set_ylim([-180, 180])
        ax[0, i].set_yticks([-90, 0, 90])
        ax[0, i].set_xticks([])
        ax[0, i].axhline(y=angle, color="r", linestyle=":")
        ax[0, i].legend(loc="best", handlelength=0)

    for i in range(5):
        for j, angle in enumerate([phi, psi]):
            ax[i + 1, j].plot(phi_psi_angles.T[j][i])
            ax[i + 1, j].set_ylim([-180, 180])
            ax[i + 1, j].set_yticks([-90, 0, 90])
            ax[i + 1, j].set_xticks([])
            ax[i + 1, j].axhline(y=angle, color="r", linestyle=":")

    fig.suptitle("Phi/Psi angles")
    plt.savefig(f"{output_dir}/phi_psi_angles.png")
    plt.close()


def plot_scatter(phi_psi_angles, output_dir):
    import matplotlib.pyplot as plt

    # Plot phi/psi angles as a scatter plot
    fig, ax = plt.subplots()
    ax.scatter(phi_psi_angles.T[0], phi_psi_angles.T[1])
    ax.set_xlim([-180, 180])
    ax.set_ylim([-180, 180])
    ax.grid(True)
    ax.set_xlabel("Phi")
    ax.set_ylabel("Psi")
    fig.suptitle("Phi/Psi angles")
    plt.savefig(f"{output_dir}/phi_psi_scatter.png")
    plt.close()


@task
def trajectory_dihedral(tpr, xtc, output_dir, phi_angle, psi_angle, plot=True):
    import logging
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran
    from math import dist

    traj_means = []
    distances = []
    u = Universe(tpr, xtc)
    protein = u.select_atoms("protein")
    rama = Ramachandran(protein).run()
    phi_psi_angles = rama.results.angles
    for i, frame in enumerate(phi_psi_angles):
        means = frame.mean(axis=0)
        distance = dist(means, [float(phi_angle), float(psi_angle)])
        logging.info(
            f"Frame {i} Phi/Psi angle means: {means} is {distance} from target [{phi_angle}, {psi_angle}]"
        )
        logging.info(f"Frame {i} Phi/Psi angles: {frame}")
        traj_means.append(means.tolist())
        distances.append(distance)

    if plot:
        plot_timeseries(phi_psi_angles, phi_angle, psi_angle, output_dir)
        plot_scatter(phi_psi_angles, output_dir)

    return traj_means[-1]


with DAG(
    "pull",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
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
        "output_dir": "pulling",
        "output_fn": "result",
        "expected_output": "dihedrals.json",
        "index_fn": "dihedrals.ndx",
        "force_constant": 500,
        "mdp_options": {"nsteps": 2000, "nstxout_compressed": 500, "dt": 0.001},
        "phi_angle": -50,
        "psi_angle": -50,
    },
) as dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data="{{ params.inputs.gro.ref_data }}",
    )
    dih_ndx = make_ndx_dihedrals(
        gro=input_gro,
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.index_fn }}",
    )
    mdp_updates = mdp_update_params.override(task_id="initial_mdp_updates")(
        dih_ndx,
        force_constant="{{ params.force_constant }}",
        phi_angle="{{ params.phi_angle }}",
        psi_angle="{{ params.psi_angle }}",
        extra_updates="{{ params.mdp_options }}",
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data="{{ params.inputs.top.ref_data }}",
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_sim_update")(
        mdp_json_file_path=input_mdp,
        update_dict=mdp_updates,
    )
    grompp = ResourceGmxOperator(
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
        output_files={"-o": "{{ params.output_fn }}.tpr"},
        output_dir="{{ params.output_dir }}",
    )

    mdrun = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 4,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-c": "{{ params.output_fn }}.gro",
            "-x": "{{ params.output_fn }}.xtc",
        },
        output_dir="{{ params.output_dir }}",
    )

    traj_dihedrals = trajectory_dihedral.override(task_id="trajectory_dihedrals")(
        tpr="{{ ti.xcom_pull(task_ids='grompp')['-o'] }}",
        xtc="{{ ti.xcom_pull(task_ids='mdrun')['-x'] }}",
        output_dir="{{ params.output_dir }}",
        phi_angle="{{ params.phi_angle }}",
        psi_angle="{{ params.psi_angle }}",
    )
    grompp >> mdrun >> traj_dihedrals

    add_sim_info = add_to_dataset.override(task_id="add_sim_info")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.expected_output }}",
        new_data={
            "gro": "{{ ti.xcom_pull(task_ids='mdrun')['-c'] }}",
            "xtc": "{{ ti.xcom_pull(task_ids='mdrun')['-x'] }}",
            "tpr": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}",
            "next_gro": "{{ ti.xcom_pull(task_ids='mdrun')['-c'] }}",
            "next_gro_means": traj_dihedrals,
            "force_constant": "{{ params.force_constant }}",
            "point": ["{{ params.phi_angle }}", "{{ params.psi_angle }}"],
        },
        new_data_keys=["{{ params.force_constant }}"],
    )
    mdrun >> add_sim_info
