from airflow import DAG
from airflow.decorators import task

from airflowHPC.dags.swarms_tasks import iterations_completed


from airflowHPC.dags.tasks import (
    json_from_dataset_path,
)


@task
def get_sim_cvs(data, num_completed_iters):
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran
    import numpy as np

    keys = list(data.keys())
    iterations_to_use = [
        k for k in keys if int(k.split("iteration_")[1]) <= num_completed_iters
    ]
    cv_info = {}
    for iteration in iterations_to_use:
        cv_iteration_info = []
        iteration_data = data[iteration]
        iteration_data.pop("params")
        for swarm_idx, swarm_info in iteration_data.items():
            for sim_info in swarm_info["sims"]:
                tpr = sim_info["tpr"]
                xtc = sim_info["xtc"]
                u = Universe(tpr, xtc)
                selected_residues = u.select_atoms("protein")
                rama = Ramachandran(selected_residues).run()
                angles = rama.results.angles
                cv_iteration_info.append(angles)
        cv_info[iteration] = np.concatenate(cv_iteration_info, axis=0).tolist()
    return cv_info


@task
def plot_free_energy(cv_info, output_dir):
    import logging
    import matplotlib.pyplot as plt
    import numpy as np

    k_B = 1.987204e-3  # Boltzmann constant in kJ/(mol*K)
    T = 300  # Temperature in Kelvin

    # Convert cv_info to numpy array of points
    cv_info = np.concatenate(np.concatenate(list(cv_info.values()), axis=0), axis=0)

    # Calculate histogram
    hist, xedges, yedges = np.histogram2d(
        cv_info[:, 0], cv_info[:, 1], bins=50, density=True
    )
    logging.info(f"xedges: {xedges}")
    logging.info(f"yedges: {yedges}")

    # Calculate free energy
    free_energy = -k_B * T * np.log(hist)
    min_positive_free_energy = np.min(free_energy[free_energy > 0])
    free_energy[hist == 0] = np.nan
    free_energy = np.nan_to_num(free_energy, nan=np.nan, posinf=np.nan, neginf=np.nan)
    free_energy -= min_positive_free_energy
    logging.info(f"Free energy: {free_energy}")

    # Plot free energy
    plt.figure(figsize=(8, 6))
    num_contours = round(np.max(free_energy[free_energy > 0])) * 5
    logging.info(f"Number of contours: {num_contours}")
    plt.contourf(xedges[:-1], yedges[:-1], free_energy.T, levels=20, cmap="viridis")
    plt.colorbar(label="Free Energy (kcal/mol)")
    plt.xlabel("Phi")
    plt.ylabel("Psi")
    plt.xlim(-180, 180)
    plt.ylim(-180, 180)
    plt.xticks([-180, -90, 0, 90, 180])
    plt.yticks([-180, -90, 0, 90, 180])
    plt.title("Free Energy Landscape")
    plt.savefig(f"{output_dir}/free_energy_landscape.png")
    plt.close()


@task
def plot_paths(cv_info, output_dir):
    import matplotlib.pyplot as plt
    import numpy as np

    num_iterations = len(cv_info.keys())
    ncols = 3
    nrows = (num_iterations + ncols - 1) // ncols
    fig, ax = plt.subplots(nrows, ncols, figsize=(15, 5 * nrows))
    ax = ax.flatten()

    for i, (iteration, iter_info) in enumerate(cv_info.items()):
        cmap = plt.cm.get_cmap("viridis")
        colors = cmap(np.linspace(0, 1, len(iter_info)))
        for j, sim_info in enumerate(iter_info):
            sim_info = np.array(sim_info)
            ax[i].scatter(
                sim_info[:, 0],
                sim_info[:, 1],
                s=10,
                marker="o",
                alpha=0.4,
                color=colors[j],
            )
        ax[i].set_xlim(-180, 180)
        ax[i].set_ylim(-180, 180)
        ax[i].set_xticks([-180, -90, 0, 90, 180])
        ax[i].set_yticks([-180, -90, 0, 90, 180])
        ax[i].set_xlabel("Phi")
        ax[i].set_ylabel("Psi")
        ax[i].set_aspect("equal")
        ax[i].set_title(f"{iteration}")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/paths.png")
    plt.close()


with DAG(
    dag_id="swarms_postprocessing",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "output_dir": "swarms",
        "expected_output": "swarms.json",
        "iterations_to_consider": 12,
    },
) as swarms_postprocessing:
    prev_results = json_from_dataset_path.override(task_id="get_iteration_params")(
        dataset_path="{{ params.output_dir }}/{{ params.expected_output }}",
        allow_missing=True,
    )
    num_completed_iters = iterations_completed(
        prev_results, "{{ params.iterations_to_consider }}"
    )
    cv_data = get_sim_cvs(prev_results, num_completed_iters)
    plot_free_energy(cv_data, "{{ params.output_dir }}")
    plot_paths(cv_data, "{{ params.output_dir }}")
