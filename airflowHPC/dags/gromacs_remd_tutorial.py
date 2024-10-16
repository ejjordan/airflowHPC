import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

from airflowHPC.operators import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflowHPC.dags.tasks import (
    get_file,
    run_gmxapi,
    branch_task,
    prepare_gmx_input,
    update_gmx_input,
    dict_from_xcom_dicts,
)
from gmxapi.commandline import cli_executable

try:
    gmx_executable = cli_executable()
except:
    gmx_executable = "gmx_mpi"

NUM_SIMULATIONS = 4


@task
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
    imgplot = plt.imshow(bg_image, aspect="auto", extent=(-209, 195, -207, 190))

    plt.show()
    plt.savefig(output_file)


@task
def extract_edr_info(edr_file, field):
    import pyedr

    edr = pyedr.edr_to_dict(edr_file)
    return edr[field].tolist()


@task
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
    "system_setup",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "remd",
        "step_dir": "prep",
    },
) as system_setup:
    system_setup.doc = """Initial setup of a system for replica exchange."""

    input_pdb = get_file.override(task_id="get_pdb")(
        input_dir="ala_tripeptide_remd", file_name="ala_tripeptide.pdb"
    )
    pdb2gmx = run_gmxapi.override(task_id="pdb2gmx")(
        args=["pdb2gmx", "-ff", "charmm27", "-water", "tip3p"],
        input_files={"-f": input_pdb},
        output_files={"-o": "alanine.gro", "-p": "topol.top", "-i": "posre.itp"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    editconf = run_gmxapi.override(task_id="editconf")(
        args=["editconf", "-c", "-box", "3", "3", "3"],
        input_files={"-f": pdb2gmx["-o"]},
        output_files={"-o": "alanine_box.gro"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    # gmx solvate does not allow specifying different file names for input and output top files.
    # Here we rely on the fact that solvate overwrites the input top file with the solvated top file.
    # Thus, after solvate, pdb2gmx["-p"] is what should be solvate["-p"].
    solvate = run_gmxapi.override(task_id="solvate")(
        args=["solvate"],
        input_files={"-cp": editconf["-o"], "-cs": "spc216.gro", "-p": pdb2gmx["-p"]},
        output_files={"-o": "alanine_solv.gro"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    mdp_json_em = get_file.override(task_id="get_min_mdp_json")(
        input_dir="mdp", file_name="min.json"
    )
    mdp_em = update_write_mdp_json_as_mdp_from_file(mdp_json_file_path=mdp_json_em)
    grompp_em = run_gmxapi.override(task_id="grompp_em")(
        args=["grompp"],
        input_files={"-f": mdp_em, "-c": solvate["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "em.tpr"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    mdrun_em = run_gmxapi.override(task_id="mdrun_em")(
        args=["mdrun", "-v", "-deffnm", "em"],
        input_files={"-s": grompp_em["-o"]},
        output_files={"-c": "em.gro"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    mdp_json_nvt = get_file.override(task_id="get_nvt_mdp_json")(
        input_dir="mdp", file_name="nvt.json"
    )
    mdp_nvt = update_write_mdp_json_as_mdp_from_file(mdp_json_file_path=mdp_json_nvt)
    grompp_nvt = run_gmxapi.override(task_id="grompp_nvt")(
        args=["grompp"],
        input_files={
            "-f": mdp_nvt,
            "-c": mdrun_em["-c"],
            "-r": mdrun_em["-c"],
            "-p": pdb2gmx["-p"],
        },
        output_files={"-o": "nvt.tpr"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )
    mdrun_nvt = run_gmxapi.override(task_id="mdrun_nvt")(
        args=["mdrun", "-v", "-deffnm", "nvt"],
        input_files={"-s": grompp_nvt["-o"]},
        output_files={"-c": "nvt.gro"},
        output_dir="{{ params.output_dir }}/{{ params.step_dir }}",
    )


with DAG(
    dag_id="equilibrate",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "remd",
        "step_dir": "equil",
    },
) as equilibrate:
    equilibrate.doc = """Equilibration of a system for replica exchange."""

    gro_equil = get_file.override(task_id="get_equil_gro")(
        input_dir="{{ params.output_dir }}/prep",
        file_name="nvt.gro",
        use_ref_data=False,
    )
    top_equil = get_file.override(task_id="get_equil_top")(
        input_dir="{{ params.output_dir }}/prep",
        file_name="topol.top",
        use_ref_data=False,
    )
    mdp_json_equil = get_file.override(task_id="get_equil_mdp_json")(
        input_dir="mdp", file_name="npt.json"
    )
    mdp_equil = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=mdp_json_equil
    ).expand(
        update_dict=[{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}]
    )
    grompp_input_list_equil = prepare_gmx_input(
        args=["grompp"],
        input_files={
            "-f": mdp_equil,
            "-c": gro_equil,
            "-r": gro_equil,
            "-p": top_equil,
        },
        output_files={"-o": "equil.tpr"},
        output_path_parts=["{{ params.output_dir }}", "{{ params.step_dir }}", "sim_"],
        num_simulations=NUM_SIMULATIONS,
    )
    grompp_equil = ResourceGmxOperatorDataclass.partial(
        task_id="grompp_equil",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_equil)
    mdrun_input = (
        update_gmx_input.override(task_id="mdrun_prepare_equil")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-e": "ener.edr",
                "-c": "result.gro",
                "-x": "result.xtc",
                "-cpo": "result.cpt",
            },
        )
        .expand(gmx_output=grompp_equil.output)
    )
    mdrun_result = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun_equil",
        executor_config={
            "mpi_ranks": 2,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input)
    key_name = "edr"
    dataset_dict = dict_from_xcom_dicts.override(task_id="make_dataset_dict")(
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun_equil', key='return_value')}}",
        dict_structure={
            key_name: "-e",
        },
    )
    mdrun_result >> dataset_dict
    edr_files = dataset_dict.map(lambda x: x[key_name])
    potential_energies_list_equil = (
        extract_edr_info.override(task_id="gmx_ener_equil")
        .partial(field="Potential")
        .expand(edr_file=edr_files)
    )
    hist_equil = plot_histograms.override(task_id="plot_histograms_equil")(
        data_list=potential_energies_list_equil,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="{{ params.output_dir }}/{{ params.step_dir }}/potential_energy_histogram_equil.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )


with DAG(
    dag_id="simulate_ala",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "remd",
        "step_dir": "sim",
    },
) as simulate_ala:
    simulate_ala.doc = """Simulation of a system for replica exchange."""

    gro_sim = (
        get_file.override(task_id="get_sim_gro")
        .partial(file_name="result.gro", use_ref_data=False)
        .expand(
            input_dir=[
                "{{ params.output_dir }}" + f"/equil/sim_{i}"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    top_sim = get_file.override(task_id="get_sim_top")(
        input_dir="{{ params.output_dir }}/prep",
        file_name="topol.top",
        use_ref_data=False,
    )
    cpt_sim = (
        get_file.override(task_id="get_sim_cpt")
        .partial(file_name="result.cpt", use_ref_data=False)
        .expand(
            input_dir=[
                "{{ params.output_dir }}" + f"/equil/sim_{i}"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    mdp_json_sim = get_file.override(task_id="get_sim_mdp_json")(
        input_dir="mdp", file_name="sim.json"
    )
    mdp_sim = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=mdp_json_sim
    ).expand(
        update_dict=[{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}]
    )
    grompp_input_list_sim = prepare_gmx_input(
        args=["grompp"],
        input_files={
            "-f": mdp_sim,
            "-c": gro_sim,
            "-r": gro_sim,
            "-p": top_sim,
            "-t": cpt_sim,
        },
        output_files={"-o": "sim.tpr"},
        output_path_parts=["{{ params.output_dir }}", "{{ params.step_dir }}", "sim_"],
        num_simulations=NUM_SIMULATIONS,
    )
    grompp_sim = ResourceGmxOperatorDataclass.partial(
        task_id="grompp_sim",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list_sim)
    mdrun_sim = BashOperator(
        bash_command=f"mpirun -np 4 {gmx_executable} mdrun -replex 100 -multidir "
        + "{{ params.output_dir }}/{{ params.step_dir }}/"
        + f"/sim_[0123] -s sim.tpr",
        task_id="mdrun_sim",
        cwd=os.path.curdir,
    )
    potential_energy_list_sim = (
        extract_edr_info.override(task_id="gmx_ener_sim")
        .partial(field="Potential")
        .expand(
            edr_file=[
                "{{ params.output_dir }}/{{ params.step_dir }}/" + f"sim_{i}/ener.edr"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    hist_sim = plot_histograms.override(task_id="plot_histograms_sim")(
        data_list=potential_energy_list_sim,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="{{ params.output_dir }}/{{ params.step_dir }}/potential_energy_histogram_sim.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )
    get_final_tpr = get_file.override(task_id="get_final_tpr")(
        input_dir="{{ params.output_dir }}/{{ params.step_dir }}/sim_0",
        file_name="sim.tpr",
        use_ref_data=False,
    )
    get_final_xtc = get_file.override(task_id="get_final_xtc")(
        input_dir="{{ params.output_dir }}/{{ params.step_dir }}/sim_0",
        file_name="traj_comp.xtc",
        use_ref_data=False,
    )
    plot_ramachandran = plot_ramachandran_residue.override(task_id="plot_ramachandran")(
        tpr_file=get_final_tpr,
        xtc_file=get_final_xtc,
        resnum=2,
        output_file="{{ params.output_dir }}/{{ params.step_dir }}/ramaPhiPsiALA2.png",
    )
    grompp_sim >> mdrun_sim >> potential_energy_list_sim >> hist_sim
    mdrun_sim >> (get_final_tpr, get_final_xtc) >> plot_ramachandran


with DAG(
    dag_id="coordinate",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "remd",
        "setup_dir": "prep",
        "equilibrate_dir": "equil",
        "simulate_dir": "sim",
    },
) as coordinate:
    coordinate.doc = """Reworking of gromacs tutorial for replica exchange MD.
        Source: https://gitlab.com/gromacs/online-tutorials/tutorials-in-progress.git"""

    is_setup_done = get_file.override(task_id="is_setup_done")(
        input_dir="{{ params.output_dir }}/{{ params.setup_dir }}",
        file_name="nvt.gro",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_setup = TriggerDagRunOperator(
        task_id="trigger_setup",
        trigger_dag_id="system_setup",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf={
            "output_dir": "{{ params.output_dir }}",
            "step_dir": "{{ params.setup_dir }}",
        },
    )
    setup_done = EmptyOperator(task_id="setup_done", trigger_rule="none_failed")
    setup_done_branch = branch_task.override(task_id="setup_done_branch")(
        truth_value=is_setup_done,
        task_if_true=setup_done.task_id,
        task_if_false=trigger_setup.task_id,
    )

    is_equil_done = (
        get_file.override(task_id="is_equil_done")
        .partial(
            file_name="equil.tpr",
            use_ref_data=False,
            check_exists=True,
        )
        .expand(
            input_dir=[
                "{{ params.output_dir }}/{{ params.equilibrate_dir }}" + f"/sim_{i}"
                for i in range(NUM_SIMULATIONS)
            ]
        )
    )
    trigger_equil = TriggerDagRunOperator(
        task_id="trigger_equil",
        trigger_dag_id="equilibrate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf={
            "output_dir": "{{ params.output_dir }}",
            "step_dir": "{{ params.equilibrate_dir }}",
        },
    )
    equil_done = EmptyOperator(task_id="equil_done", trigger_rule="none_failed")
    equil_done_branch = branch_task.override(task_id="equil_done_branch")(
        truth_value=is_equil_done,
        task_if_true=equil_done.task_id,
        task_if_false=trigger_equil.task_id,
    )

    is_sim_done = get_file.override(task_id="is_sim_done")(
        input_dir="{{ params.output_dir }}/{{ params.simulate_dir }}/sim_0",
        file_name="sim.tpr",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_sim = TriggerDagRunOperator(
        task_id="trigger_sim",
        trigger_dag_id="simulate_ala",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        conf={
            "output_dir": "{{ params.output_dir }}",
            "step_dir": "{{ params.simulate_dir }}",
        },
    )
    sim_done = EmptyOperator(task_id="sim_done", trigger_rule="none_failed")
    sim_done_branch = branch_task.override(task_id="sim_done_branch")(
        truth_value=is_sim_done,
        task_if_true=sim_done.task_id,
        task_if_false=trigger_sim.task_id,
    )

    is_setup_done >> setup_done_branch >> [trigger_setup, setup_done] >> is_equil_done
    is_equil_done >> equil_done_branch >> [trigger_equil, equil_done] >> is_sim_done
    is_sim_done >> sim_done_branch >> [trigger_sim, sim_done]
