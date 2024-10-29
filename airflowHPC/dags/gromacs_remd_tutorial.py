import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

from airflowHPC.operators import ResourceGmxOperatorDataclass, ResourceBashOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflowHPC.dags.tasks import (
    get_file,
    branch_task,
    prepare_gmx_input,
    run_if_needed,
    verify_files,
    run_if_false,
    json_from_dataset_path,
)
from gmxapi.commandline import cli_executable

try:
    gmx_executable = cli_executable()
except:
    gmx_executable = "gmx_mpi"

NUM_SIMULATIONS = 4


@task(trigger_rule="none_failed")
def plot_ramachandran_residue(tpr_file, xtc_file, resnum, output_file):
    import matplotlib.pyplot as plt
    import os
    from airflowHPC.data import data_dir
    from MDAnalysis import Universe

    u = Universe(tpr_file, xtc_file)
    selected_residues = u.select_atoms(f"resid {resnum}")

    from MDAnalysis.analysis.dihedrals import Ramachandran

    phi_psi_angles = Ramachandran(selected_residues).run()
    phi_angles, psi_angles = phi_psi_angles.angles.T

    plt.figure(figsize=(8, 8))
    plt.scatter(phi_angles, psi_angles, alpha=0.7)
    plt.title("Ramachandran Plot")
    plt.xlabel("Phi (degrees)")
    plt.ylabel("Psi (degrees)")
    plt.grid(True)

    plt.xlim([-180, 180])
    plt.ylim([-180, 180])
    plt.gca().set_aspect("equal", adjustable="box")

    plt.axhline(0, color="black", linewidth=0.5)
    plt.axvline(0, color="black", linewidth=0.5)

    plot_dir = os.path.join(os.path.join(data_dir, "ala_tripeptide_remd"))
    bg_image = plt.imread(
        fname=os.path.join(plot_dir, "Ramachandran_plot_original_outlines.jpg")
    )
    plt.imshow(bg_image, aspect="auto", extent=(-209, 195, -207, 190))

    plt.show()
    plt.savefig(output_file)


@task(trigger_rule="none_failed")
def extract_edr_info(edr_file, field):
    import pyedr

    edr = pyedr.edr_to_dict(edr_file)
    return edr[field].tolist()


@task(trigger_rule="none_failed")
def extract_edr_data(edr_dataset, field):
    import pyedr

    edr_data = []
    for edr in edr_dataset:
        edr_data.append(pyedr.edr_to_dict(edr["edr"])[field].tolist())
    return edr_data


@task(trigger_rule="none_failed")
def plot_histograms(data_list, labels, hatch_list, xlabel, title, output_file):
    import matplotlib.pyplot as plt
    import logging

    for data in data_list:
        plt.hist(
            data,
            bins=5,
            alpha=0.5,
            hatch=hatch_list.pop(),
            label=labels.pop(),
            edgecolor="black",
            density=True,
        )
    plt.xlabel(xlabel)
    plt.ylabel("Density")
    plt.title(title)
    plt.legend()
    plt.savefig(output_file)
    logging.info(f"Saved histogram to {output_file}")


with DAG(
    dag_id="remd_demo",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "remd",
        "setup_dir": "prep",
        "minimize_dir": "em",
        "nvt_equil_dir": "nvt_equil",
        "npt_equil_dir": "npt_equil",
        "simulate_dir": "sim",
        "mdp_options": [{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}],
    },
) as coordinate:
    coordinate.doc = """Reworking of gromacs tutorial for replica exchange MD.
        Source: https://gitlab.com/gromacs/online-tutorials/tutorials-in-progress.git"""

    setup_params = {
        "inputs": {
            "pdb": {
                "directory": "ala_tripeptide_remd",
                "filename": "ala_tripeptide.pdb",
            }
        },
        "output_dir": "{{ params.output_dir }}/{{ params.setup_dir }}",
        "box_size": 3,
        "force_field": "charmm27",
        "water_model": "tip3p",
        "ion_concentration": 0,
        "expected_output": "system_prepared.gro",
    }
    setup = run_if_needed.override(group_id="prepare_system")(
        dag_id="prepare_system", dag_params=setup_params
    )

    minimize_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "min.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.setup_dir }}",
                "filename": "system_prepared.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup_dir }}",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.minimize_dir }}",
        "expected_output": "em.gro",
    }
    minimize = run_if_needed.override(group_id="minimize")(
        dag_id="simulate_no_cpt",
        dag_params=minimize_params,
        dag_display_name="minimize",
    )

    nvt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "nvt.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.minimize_dir }}",
                "filename": "em.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup_dir }}",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.nvt_equil_dir }}",
        "expected_output": "nvt.gro",
    }
    nvt_equil = run_if_needed.override(group_id="nvt_equil")(
        dag_id="simulate_no_cpt", dag_params=nvt_params, dag_display_name="nvt_equil"
    )

    npt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "npt.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.nvt_equil_dir }}",
                "filename": "nvt.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup_dir }}",
                "filename": "system_prepared.top",
            },
        },
        "mdp_options": "{{ params.mdp_options }}",
        "step_number": 0,
        "output_dir": "{{ params.output_dir }}/{{ params.npt_equil_dir }}",
        "expected_output": "npt.gro",
        "output_dataset_structure": {
            "edr": "-e",
        },
    }
    npt_equil_has_run = verify_files.override(task_id="npt_equil_has_run")(
        input_dir="{{ params.output_dir }}/{{ params.npt_equil_dir }}",
        filename="npt.gro",
        mdp_options="{{ params.mdp_options }}",
        step_number=None,
    )
    npt_equil = run_if_false.override(group_id="npt_equil")(
        dag_id="simulate_expand",
        dag_params=npt_params,
        truth_value=npt_equil_has_run,
        wait_for_completion=True,
        dag_display_name="npt_equil",
    )

    edr_data = json_from_dataset_path.override(task_id="edr_data")(
        dataset_path="{{ params.output_dir }}/{{ params.npt_equil_dir }}/npt.json",
    )
    npt_equil >> edr_data

    potential_energies_list_equil = extract_edr_data.override(task_id="gmx_ener_equil")(
        field="Potential", edr_dataset=edr_data
    )
    hist_equil = plot_histograms.override(task_id="plot_histograms_equil")(
        data_list=potential_energies_list_equil,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="{{ params.output_dir }}/{{ params.npt_equil_dir }}/potential_energy_histogram_equil.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )

    sim_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "sim.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.npt_equil_dir }}",
                "filename": "npt.gro",
            },
            "cpt": {
                "directory": "{{ params.output_dir }}/{{ params.npt_equil_dir }}",
                "filename": "npt.cpt",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup_dir }}",
                "filename": "system_prepared.top",
            },
        },
        "mdp_options": "{{ params.mdp_options }}",
        "output_dir": "{{ params.output_dir }}/{{ params.simulate_dir }}",
        "expected_output": "sim.gro",
    }

    is_sim_done = get_file.override(task_id="is_sim_done")(
        input_dir="{{ params.output_dir }}/{{ params.simulate_dir }}/sim_0",
        file_name="sim.tpr",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_sim = TriggerDagRunOperator(
        task_id="trigger_sim",
        trigger_dag_id="simulate_multidir",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf=sim_params,
    )
    sim_done = EmptyOperator(task_id="sim_done", trigger_rule="none_failed")
    sim_done_branch = branch_task.override(task_id="sim_done_branch")(
        truth_value=is_sim_done,
        task_if_true=sim_done.task_id,
        task_if_false=trigger_sim.task_id,
    )

    potential_energy_list_sim = (
        extract_edr_info.override(task_id="gmx_ener_sim")
        .partial(field="Potential")
        .expand(
            edr_file=[
                "{{ params.output_dir }}/{{ params.simulate_dir }}/"
                + f"sim_{i}/sim.edr"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    hist_sim = plot_histograms.override(task_id="plot_histograms_sim")(
        data_list=potential_energy_list_sim,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="{{ params.output_dir }}/{{ params.simulate_dir }}/potential_energy_histogram_sim.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )
    get_final_tpr = get_file.override(task_id="get_final_tpr")(
        input_dir="{{ params.output_dir }}/{{ params.simulate_dir }}/sim_0",
        file_name="sim.tpr",
        use_ref_data=False,
    )
    get_final_xtc = get_file.override(task_id="get_final_xtc")(
        input_dir="{{ params.output_dir }}/{{ params.simulate_dir }}/sim_0",
        file_name="sim.xtc",
        use_ref_data=False,
    )
    plot_ramachandran = plot_ramachandran_residue.override(task_id="plot_ramachandran")(
        tpr_file=get_final_tpr,
        xtc_file=get_final_xtc,
        resnum=2,
        output_file="{{ params.output_dir }}/{{ params.simulate_dir }}/ramaPhiPsiALA2.png",
    )

    setup >> minimize >> nvt_equil >> npt_equil_has_run >> npt_equil
    npt_equil >> is_sim_done >> sim_done_branch >> [trigger_sim, sim_done]
    trigger_sim >> potential_energy_list_sim
    trigger_sim >> [get_final_tpr, get_final_xtc]
