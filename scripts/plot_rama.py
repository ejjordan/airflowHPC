import matplotlib.pyplot as plt
import os
import numpy as np
from airflowHPC.data import data_dir


def plot_gromacs():
    rama_file = os.path.join("remd_mdrun", "ramaPhiPsiALA2.xvg")
    print(rama_file)
    print(os.path.exists(rama_file))
    with open(rama_file, "r") as f:
        lines = f.readlines()
    data = []
    for line in lines:
        x, y = line.split()
        data.append((float(x), float(y)))
    phi_angles, psi_angles = zip(*data)

    # Convert angles to radians for matplotlib
    phi_angles_rad = np.deg2rad(phi_angles)
    psi_angles_rad = np.deg2rad(psi_angles)

    # Plot Ramachandran plot
    plt.figure(figsize=(8, 8))
    plt.scatter(phi_angles, psi_angles, alpha=0.7)
    plt.title("Ramachandran Plot")

    plt.text(-57, -47, "Alpha Helix", fontsize=16, color="green")
    plt.text(-119, 113, "Beta Sheet", fontsize=16, color="blue")
    plt.text(-50, -26, "3-10 Helix", fontsize=16, color="red")

    # Set plot limits and aspect ratio
    plt.xlim([-180, 180])
    plt.ylim([-180, 180])
    plt.gca().set_aspect("equal", adjustable="box")

    # Divide plot into quadrants
    plt.axhline(0, color="black", linewidth=0.5)
    plt.axvline(0, color="black", linewidth=0.5)

    # Set labels
    plt.xlabel("Phi (degrees)")
    plt.ylabel("Psi (degrees)")

    # Add grid
    plt.grid(True)

    # Add Ramachandran plot background image
    plot_dir = os.path.join(os.path.join(data_dir, "ala_tripeptide_remd"))
    bg_image = plt.imread(
        fname=os.path.join(plot_dir, "Ramachandran_plot_original_outlines.jpg")
    )
    imgplot = plt.imshow(bg_image, aspect="auto", extent=(-209, 195, -207, 190))

    # Show plot
    plt.show()


def plot_mdanalysis():
    import matplotlib.pyplot as plt
    import os
    from airflowHPC.data import data_dir
    import logging
    import MDAnalysis as mda

    # Load the topology and trajectory
    u = mda.Universe(
        "/home/joe/experiments/airflow-hpc/remd_tut/sim/step_0/sim_0/sim.tpr",
        "/home/joe/experiments/airflow-hpc/remd_tut/sim/step_0/sim_0/traj_comp.xtc",
    )

    # Select residues
    selected_residues = u.select_atoms(f"resid {2}")

    # Calculate phi and psi angles
    from MDAnalysis.analysis.dihedrals import Ramachandran

    phi_psi_angles = Ramachandran(selected_residues).run()
    # import ipdb; ipdb.set_trace()

    # Access the phi and psi angles
    phi_angles, psi_angles = phi_psi_angles.angles.T

    # Plot Ramachandran plot
    plt.figure(figsize=(8, 8))
    plt.scatter(phi_angles, psi_angles, alpha=0.7)
    plt.title("Ramachandran Plot")

    # Set plot limits and aspect ratio
    plt.xlim([-180, 180])
    plt.ylim([-180, 180])
    plt.gca().set_aspect("equal", adjustable="box")

    # Divide plot into quadrants
    plt.axhline(0, color="black", linewidth=0.5)
    plt.axvline(0, color="black", linewidth=0.5)

    # Set labels
    plt.xlabel("Phi (degrees)")
    plt.ylabel("Psi (degrees)")

    # Add grid
    plt.grid(True)

    # Add Ramachandran plot background image
    plot_dir = os.path.join(os.path.join(data_dir, "ala_tripeptide_remd"))
    bg_image = plt.imread(
        fname=os.path.join(plot_dir, "Ramachandran_plot_original_outlines.jpg")
    )
    imgplot = plt.imshow(bg_image, aspect="auto", extent=(-209, 195, -207, 190))

    # Show plot
    plt.show()
    # plt.savefig("rama.png")


plot_mdanalysis()
