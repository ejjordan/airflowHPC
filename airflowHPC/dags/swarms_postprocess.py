from airflow import DAG
from airflow.decorators import task
from airflowHPC.dags.swarms_tasks import iterations_completed
from airflowHPC.dags.tasks import (
    json_from_dataset_path,
)


@task(multiple_outputs=True)
def get_sim_cvs(data, num_completed_iters):
    import logging
    import numpy as np
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran

    logging.getLogger("MDAnalysis").setLevel(logging.WARNING)

    keys = list(data.keys())
    iterations_to_use = [
        k for k in keys if int(k.split("iteration_")[1]) <= num_completed_iters
    ]
    cv_info = {}
    for iteration in iterations_to_use:
        iteration_data = data[iteration]
        iteration_data.pop("params")
        swarms = {}
        for swarm_idx, swarm_info in iteration_data.items():
            phi_psi_point = np.round(swarm_info["phi_psi"], 2).tolist()
            swarm_sims = {}
            for sim_info in swarm_info["sims"]:
                tpr = sim_info["tpr"]
                xtc = sim_info["xtc"]
                sim_id = sim_info["simulation_id"]
                u = Universe(tpr, xtc)
                selected_residues = u.select_atoms("protein")
                rama = Ramachandran(selected_residues).run()
                angles = rama.results.angles
                angle_means = np.round(np.mean(angles, axis=1), 2).tolist()
                swarm_sims[sim_id] = angle_means
            swarms[str(phi_psi_point)] = swarm_sims
        cv_info[iteration] = swarms
    return cv_info


@task
def plot_free_energy(cv_info, output_dir):
    import logging
    import matplotlib.pyplot as plt
    import numpy as np

    k_B = 1.987204e-3  # Boltzmann constant in kJ/(mol*K)
    T = 300  # Temperature in Kelvin

    sim_data = []
    for i, (iteration, iter_info) in enumerate(cv_info.items()):
        logging.debug(f"iter_info: {iter_info}")
        for j, (point, swarm_info) in enumerate(iter_info.items()):
            logging.debug(f"swarm_info: {swarm_info}")
            logging.debug(f"point: {point}")
            for sim_id, sim_info in swarm_info.items():
                logging.debug(f"sim_info: {sim_info}")
                sim_data.append(sim_info)

    sim_data = np.concatenate(sim_data, axis=0)

    # Calculate histogram
    hist, xedges, yedges = np.histogram2d(
        sim_data[:, 0], sim_data[:, 1], bins=50, density=True
    )

    # Calculate free energy
    free_energy = -k_B * T * np.log(hist)
    min_positive_free_energy = np.min(free_energy[free_energy > 0])
    free_energy[hist == 0] = np.nan
    free_energy = np.nan_to_num(free_energy, nan=np.nan, posinf=np.nan, neginf=np.nan)
    free_energy -= min_positive_free_energy
    logging.debug(f"Free energy: {free_energy}")

    # Plot free energy
    plt.figure(figsize=(8, 6))
    num_contours = round(np.max(free_energy[free_energy > 0])) * 5
    logging.info(f"Number of contours: {num_contours}")
    plt.contourf(
        xedges[:-1], yedges[:-1], free_energy.T, levels=num_contours, cmap="viridis"
    )
    plt.colorbar(label="Free Energy (kcal/mol)")
    plt.xlabel("Phi")
    plt.ylabel("Psi")
    plt.grid(True)
    plt.title("Free Energy Landscape")
    plt.savefig(f"{output_dir}/free_energy_landscape.png")
    plt.close()


@task
def plot_paths(cv_info, start_iteration, end_iteration, output_dir):
    import matplotlib.pyplot as plt
    import numpy as np
    import logging
    import ast

    keys = list(cv_info.keys())
    iterations_to_use = [
        k
        for k in keys
        if int(k.split("iteration_")[1]) <= end_iteration
        and int(k.split("iteration_")[1]) >= start_iteration
    ]
    logging.info(f"iterations_to_use: {iterations_to_use}")

    num_iterations = len(iterations_to_use)
    ncols = 3
    nrows = (num_iterations + ncols - 1) // ncols
    fig, ax = plt.subplots(nrows, ncols, figsize=(15, 5 * nrows))
    ax = ax.flatten()

    for i, iteration in enumerate(iterations_to_use):
        iter_info = cv_info[iteration]
        cmap = plt.cm.get_cmap("viridis")
        colors = cmap(np.linspace(0, 1, len(iter_info)))
        logging.debug(f"iter_info: {iter_info}")
        for j, (point, swarm_info) in enumerate(iter_info.items()):
            logging.debug(f"swarm_info: {swarm_info}")
            logging.debug(f"point: {point}")
            point = ast.literal_eval(point)
            ax[i].scatter(
                point[0],
                point[1],
                s=50,
                marker="x",
                color=colors[j],
            )
            for sim_id, sim_info in swarm_info.items():
                logging.debug(f"sim_info: {sim_info}")
                sim_info = np.array(sim_info)
                ax[i].scatter(
                    sim_info[:, 0],
                    sim_info[:, 1],
                    s=10,
                    marker="o",
                    alpha=0.4,
                    color=colors[j],
                )
        ax[i].set_xlabel("Phi")
        ax[i].set_ylabel("Psi")
        ax[i].set_aspect("equal")
        ax[i].set_title(f"{iteration}")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/paths.png")
    plt.close()


@task
def plot_vector_field(cv_info, start_iteration, end_iteration, output_dir, plot_sims):
    import numpy as np
    import logging
    import matplotlib.pyplot as plt
    import ast

    keys = list(cv_info.keys())
    iterations_to_use = [
        k
        for k in keys
        if int(k.split("iteration_")[1]) <= end_iteration
        and int(k.split("iteration_")[1]) >= start_iteration
    ]
    logging.info(f"iterations_to_use: {iterations_to_use}")

    swarm_avg_vectors = {}
    swarm_sim_vectors = {}
    for i, iteration in enumerate(iterations_to_use):
        iter_info = cv_info[iteration]
        logging.debug(f"iter_info: {iter_info}")
        swarm_avg_vectors[iteration] = {}
        swarm_sim_vectors[iteration] = {}
        for j, (point, swarm_info) in enumerate(iter_info.items()):
            logging.debug(f"swarm_info: {swarm_info}")
            logging.debug(f"point: {point}")
            sim_vecs = []
            for sim_info in np.array(list(swarm_info.values())):
                sim_vecs.append([sim_info[0].tolist(), sim_info[-1].tolist()])
            swarm_avg = np.average(np.array(sim_vecs), axis=0)
            logging.debug(f"swarm_avg: {swarm_avg}")
            swarm_avg_vectors[iteration][point] = swarm_avg.tolist()
            swarm_sim_vectors[iteration][point] = sim_vecs

    logging.info(f"swarm_avg_vectors: {swarm_avg_vectors}")
    logging.info(f"swarm_sim_vectors: {swarm_sim_vectors}")

    num_iterations = len(iterations_to_use)
    ncols = 4 if num_iterations <= 12 else 5
    nrows = (num_iterations + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 5 * nrows))
    fig.set_tight_layout(True)
    axes = axes.flatten()

    for i, iteration in enumerate(iterations_to_use):
        iter_info = cv_info[iteration]
        ax = axes[i]
        U_range = [0, 0]
        V_range = [0, 0]
        for j, (point, swarm_info) in enumerate(iter_info.items()):
            swarm_avg = swarm_avg_vectors[iteration][point]
            swarm_sim = swarm_sim_vectors[iteration][point]
            swarm_avg = np.array(swarm_avg)
            swarm_sim = np.array(swarm_sim)
            point = ast.literal_eval(point)
            ax.plot(point[0], point[1], color="black", marker="o")
            if plot_sims:
                X = [v[0][0] for v in swarm_sim]
                Y = [v[0][1] for v in swarm_sim]
                U = [v[1][0] - v[0][0] for v in swarm_sim]
                V = [v[1][1] - v[0][1] for v in swarm_sim]
                ax.quiver(
                    X, Y, U, V, angles="xy", scale_units="xy", scale=1, width=0.001
                )
                U_range = [
                    min(min([v[1][0] for v in swarm_sim]), U_range[0]),
                    max(max([v[1][0] for v in swarm_sim]), U_range[1]),
                ]
                V_range = [
                    min(min([v[1][1] for v in swarm_sim]), V_range[0]),
                    max(max([v[1][1] for v in swarm_sim]), V_range[1]),
                ]
            else:
                U_range = [
                    min(float(swarm_avg[1][0]), U_range[0]),
                    max(float(swarm_avg[1][0]), U_range[1]),
                ]
                V_range = [
                    min(float(swarm_avg[1][1]), V_range[0]),
                    max(float(swarm_avg[1][1]), V_range[1]),
                ]
            X = swarm_avg[0][0]
            Y = swarm_avg[0][1]
            U = swarm_avg[1][0] - swarm_avg[0][0]
            V = swarm_avg[1][1] - swarm_avg[0][1]
            ax.quiver(X, Y, U, V, angles="xy", scale_units="xy", scale=1, color="red")
            ax.set_xlabel("Phi")
            ax.set_ylabel("Psi")
            ax.set_title(f"Iteration {iteration}")
        ax.set_xlim(U_range[0] - 10, U_range[1] + 10)
        ax.set_ylim(V_range[0] - 10, V_range[1] + 10)

    plt.savefig(f"{output_dir}/trajectory_vector_field.png")
    plt.close()
    # plt.show()


@task
def plot_convergence_angles(cv_info, iteration_num, output_dir):
    import numpy as np
    import logging
    import matplotlib.pyplot as plt
    import ast

    assert f"iteration_{iteration_num}" in list(cv_info.keys())
    iteration_to_use = f"iteration_{iteration_num}"

    iter_info = cv_info[iteration_to_use]
    logging.debug(f"iter_info: {iter_info}")
    points = []
    swarm_avg_vectors = {}
    for j, (point, swarm_info) in enumerate(iter_info.items()):
        logging.debug(f"swarm_info: {swarm_info}")
        logging.debug(f"point: {point}")
        points.append(ast.literal_eval(point))
        sim_vecs = []
        for sim_info in np.array(list(swarm_info.values())):
            sim_vecs.append([sim_info[0].tolist(), sim_info[-1].tolist()])
        swarm_avg = np.average(np.array(sim_vecs), axis=0)
        logging.debug(f"swarm_avg: {swarm_avg}")
        swarm_avg_vectors[point] = swarm_avg.tolist()

    logging.info(f"points: {points}")
    logging.info(f"swarm_avg_vectors: {swarm_avg_vectors}")

    ncols = 4 if len(points) <= 12 else 5
    nrows = (len(points) + ncols) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 5 * nrows))
    fig.set_tight_layout(True)
    axes = axes.flatten()

    def vector_angle(u, v):
        u = np.array(u)
        v = np.array(v)
        cos_theta = np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))
        angle_rad = np.arccos(np.clip(cos_theta, -1.0, 1.0))
        angle_deg = np.degrees(angle_rad)
        return angle_deg

    points_T = np.transpose(points)
    x_range = np.max(points_T[0]) - np.min(points_T[0])
    y_range = np.max(points_T[1]) - np.min(points_T[1])
    long_axis = 0 if x_range > y_range else 1
    logging.info(f"Will sort points based on axis {'x' if long_axis == 0 else 'y'}")
    sorted_points = sorted(points, key=lambda x: x[long_axis])
    angle_errors = []

    cmap = plt.cm.get_cmap("viridis")
    colors = cmap(np.linspace(0, 1, len(sorted_points)))
    for j, point in enumerate(sorted_points):
        ax = axes[j + 1]
        ax.set_xlim(-120, 50)
        ax.set_ylim(-50, 120)
        swarm_avg = swarm_avg_vectors[str(point)]
        X = swarm_avg[0][0]
        Y = swarm_avg[0][1]
        U = swarm_avg[1][0] - swarm_avg[0][0]
        V = swarm_avg[1][1] - swarm_avg[0][1]
        ax.quiver(X, Y, U, V, angles="xy", scale_units="xy", scale=1, color="red")
        logging.info(f"swarm_vec: {swarm_avg}")
        swarm_vec = np.array(swarm_avg[-1]) - np.array(swarm_avg[0])
        angle_fwd, angle_rev = 180, 180
        if j > 0:
            point_rev = np.array(sorted_points[j - 1]) - np.array(sorted_points[j])
            plot_pts = np.transpose([sorted_points[j - 1], sorted_points[j]])
            angle_rev = vector_angle(swarm_vec, point_rev)
            ax.plot(
                plot_pts[0],
                plot_pts[1],
                color=colors[j],
                marker="o",
                label=f"rev: {angle_rev:.2f}",
            )
            logging.info(f"point-1: {sorted_points[j - 1]}")
        logging.info(f"point: {point}")
        if j < len(sorted_points) - 1:
            point_fwd = np.array(sorted_points[j + 1]) - np.array(sorted_points[j])
            plot_pts = np.transpose([sorted_points[j], sorted_points[j + 1]])
            angle_fwd = vector_angle(swarm_vec, point_fwd)
            ax.plot(
                plot_pts[0],
                plot_pts[1],
                color=colors[j],
                marker="o",
                label=f"fwd: {angle_fwd:.2f}",
            )
            logging.info(f"point+1: {sorted_points[j + 1]}")
        logging.info(
            f"fwd angle: {round(angle_fwd, 2)}, rev angle: {round(angle_rev, 2)}"
        )
        angle_errors.append(min(angle_fwd, angle_rev))
        ax.legend()
        ax.set_xlabel("Phi")
        ax.set_ylabel("Psi")
    angle_err = np.mean(angle_errors)
    logging.info(f"angle_sum: {angle_err} for {iteration_to_use}")

    ax = axes[0]
    U_range = [0, 0]
    V_range = [0, 0]

    for j, point in enumerate(sorted_points):
        swarm_avg = swarm_avg_vectors[str(point)]
        swarm_avg = np.array(swarm_avg)

        ax.plot(point[0], point[1], color="black", marker="o")
        U_range = [
            min(float(swarm_avg[1][0]), U_range[0]),
            max(float(swarm_avg[1][0]), U_range[1]),
        ]
        V_range = [
            min(float(swarm_avg[1][1]), V_range[0]),
            max(float(swarm_avg[1][1]), V_range[1]),
        ]
        X = swarm_avg[0][0]
        Y = swarm_avg[0][1]
        U = swarm_avg[1][0] - swarm_avg[0][0]
        V = swarm_avg[1][1] - swarm_avg[0][1]
        ax.quiver(X, Y, U, V, angles="xy", scale_units="xy", scale=1, color="red")
        ax.set_xlabel("Phi")
        ax.set_ylabel("Psi")
        ax.set_title(f"{iteration_to_use} angle error: {angle_err:.2f}")
    ax.set_xlim(U_range[0] - 10, U_range[1] + 10)
    ax.set_ylim(V_range[0] - 10, V_range[1] + 10)

    plt.savefig(f"{output_dir}/convergence_angles.png")
    plt.close()
    # plt.show()


with DAG(
    dag_id="swarms_postprocessing",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "output_dir": "swarms",
        "expected_output": "swarms.json",
        "first_iteration_to_consider": 12,
        "last_iteration_to_consider": 15,
        "show_sims": False,
    },
) as swarms_postprocessing:
    prev_results = json_from_dataset_path.override(task_id="get_iteration_params")(
        dataset_path="{{ params.output_dir }}/{{ params.expected_output }}",
        allow_missing=True,
    )
    num_completed_iters = iterations_completed(
        prev_results, "{{ params.last_iteration_to_consider }}"
    )
    cv_data = get_sim_cvs(prev_results, num_completed_iters)
    plot_free_energy(cv_data, "{{ params.output_dir }}")
    plot_paths(
        cv_info=cv_data,
        start_iteration="{{ params.first_iteration_to_consider }}",
        end_iteration=num_completed_iters,
        output_dir="{{ params.output_dir }}",
    )
    plot_vector_field(
        cv_info=cv_data,
        start_iteration="{{ params.first_iteration_to_consider }}",
        end_iteration=num_completed_iters,
        output_dir="{{ params.output_dir }}",
        plot_sims="{{ params.show_sims }}",
    )
    plot_convergence_angles(
        cv_info=cv_data,
        iteration_num=num_completed_iters,
        output_dir="{{ params.output_dir }}",
    )
