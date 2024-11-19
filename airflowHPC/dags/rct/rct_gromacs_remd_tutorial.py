from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

from airflowHPC.dags.tasks import (
    get_file,
    run_if_needed,
    verify_files,
    json_from_dataset_path,
    branch_task,
)


NUM_SIMULATIONS = 4


@task_group
def plot_hist_from_edr(edr_data, output_dir, output_file):
    potential_energies_list = extract_edr_data.override(
        task_id="gmx_ener", trigger_rule="none_skipped"
    )(field="Potential", edr_dataset=edr_data)
    hist = plot_histograms.override(
        task_id="plot_histograms", trigger_rule="none_skipped"
    )(
        data_list=potential_energies_list,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file=f"{output_dir}/{output_file}",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )
    potential_energies_list >> hist


@task_group
def plot_rama():
    get_final_tprs = (
        get_file.override(task_id="get_final_tprs")
        .partial(
            file_name="sim.tpr",
            use_ref_data=False,
        )
        .expand(
            input_dir=[
                "{{ params.output_dir }}/{{ params.simulate_dir }}/sim_" + str(i)
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    tprs = get_final_tprs.map(lambda x: x)

    get_final_xtcs = (
        get_file.override(task_id="get_final_xtcs")
        .partial(
            file_name="sim.xtc",
            use_ref_data=False,
        )
        .expand(
            input_dir=[
                "{{ params.output_dir }}/{{ params.simulate_dir }}/sim_" + str(i)
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    xtcs = get_final_xtcs.map(lambda x: x)
    tpr_xtcs = xtcs.zip(tprs)
    plot_ramachandran = plot_ramachandran_residue.override(task_id="plot_ramachandran")(
        zipped_xtc_tpr=tpr_xtcs,
        resnum=2,
        output_file="{{ params.output_dir }}/{{ params.simulate_dir }}/ramaPhiPsiALA2.png",
    )


@task(trigger_rule="none_failed")
def plot_ramachandran_residue(zipped_xtc_tpr, resnum, output_file):
    import matplotlib.pyplot as plt
    import os
    import logging
    from airflowHPC.data import data_dir
    from MDAnalysis import Universe
    from MDAnalysis.analysis.dihedrals import Ramachandran

    phi_angles = []
    psi_angles = []
    for xtc, tpr in zipped_xtc_tpr:
        logging.info(f"xtc: {xtc}, tpr: {tpr}")
        u = Universe(tpr, xtc)
        selected_residues = u.select_atoms(f"resid {resnum}")
        phi_psi_angles = Ramachandran(selected_residues).run()
        phi, psi = phi_psi_angles.results["angles"].T
        phi_angles.extend(phi.flatten().tolist())
        psi_angles.extend(psi.flatten().tolist())

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


@task.short_circuit(ignore_downstream_trigger_rules=True)
def reverse_condition(condition):
    return not condition


with DAG(
    dag_id="rct_remd_demo",
    start_date=timezone.utcnow(),
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
        dag_id="rct_prepare_system", dag_params=setup_params
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
        dag_id="rct_simulate_no_cpt",
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
        dag_id="rct_simulate_no_cpt", dag_params=nvt_params, dag_display_name="nvt_equil"
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

    edr_data = json_from_dataset_path.override(
        task_id="edr_data", trigger_rule="none_skipped"
    )(
        dataset_path="{{ params.output_dir }}/{{ params.npt_equil_dir }}/npt.json",
    )
    plot_equil = plot_hist_from_edr.override(group_id="plot_hist_equil")(
        edr_data=edr_data,
        output_dir="{{ params.output_dir }}/{{ params.npt_equil_dir }}",
        output_file="potential_energy_histogram_equil.png",
    )
    trigger_npt_equil = TriggerDagRunOperator(
        task_id="trigger_npt_equil",
        trigger_dag_id="rct_simulate_expand",
        poke_interval=2,
        conf=npt_params,
        wait_for_completion=True,
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

    npt_equil_done_branch = branch_task.override(task_id="npt_equil_done_branch")(
        truth_value=npt_equil_has_run,
        task_if_true="is_sim_done",
        task_if_false="trigger_npt_equil",
    )

    npt_equil_done_branch >> is_sim_done
    npt_equil_done_branch >> trigger_npt_equil >> edr_data >> plot_equil >> is_sim_done
    short_sim = reverse_condition.override(task_id="short_simulate")(
        condition=is_sim_done
    )
    simulate = TriggerDagRunOperator(
        task_id=f"trigger_simulate",
        trigger_dag_id="rct_simulate_multidir",
        wait_for_completion=True,
        poke_interval=2,
        trigger_rule="none_failed",
        conf=sim_params,
    )

    edr_list = (
        get_file.override(task_id="get_edrs")
        .partial(
            file_name="sim.edr",
            use_ref_data=False,
        )
        .expand(
            input_dir=[
                "{{ params.output_dir }}/{{ params.simulate_dir }}/" + f"sim_{i}/"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    edr_files = edr_list.map(lambda x: {"edr": x})
    plot_sim = plot_hist_from_edr.override(group_id="plot_hist_sim")(
        edr_data=edr_files,
        output_dir="{{ params.output_dir }}/{{ params.simulate_dir }}",
        output_file="potential_energy_histogram_sim.png",
    )
    simulate >> edr_list >> plot_sim

    rama = plot_rama()

    setup >> minimize >> nvt_equil >> npt_equil_has_run
    is_sim_done >> short_sim >> simulate >> rama
