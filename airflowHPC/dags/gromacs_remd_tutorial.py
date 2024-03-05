from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflowHPC.dags.tasks import (
    get_file,
    run_gmxapi,
    branch_task,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
    list_from_xcom,
)

NUM_SIMULATIONS = 4

with DAG(
    "system_setup",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as system_setup:
    system_setup.doc = """Reworking of gromacs tutorial for replica exchange MD.
    Source: https://gitlab.com/gromacs/online-tutorials/tutorials-in-progress.git"""

    input_pdb = get_file.override(task_id="get_pdb")(
        input_dir="ala_tripeptide_remd", file_name="ala_tripeptide.pdb"
    )
    prep_output_dir = "prep"
    pdb2gmx = run_gmxapi.override(task_id="pdb2gmx")(
        args=["pdb2gmx", "-ff", "charmm27", "-water", "tip3p"],
        input_files={"-f": input_pdb},
        output_files={"-o": "alanine.gro", "-p": "topol.top", "-i": "posre.itp"},
        output_dir=prep_output_dir,
    )
    editconf = run_gmxapi.override(task_id="editconf")(
        args=["editconf", "-c", "-box", "3", "3", "3"],
        input_files={"-f": pdb2gmx["-o"]},
        output_files={"-o": "alanine_box.gro"},
        output_dir=prep_output_dir,
    )
    solvate = run_gmxapi.override(task_id="solvate")(
        args=["solvate"],
        input_files={"-cp": editconf["-o"], "-cs": "spc216.gro"},
        output_files={"-o": "alanine_solv.gro", "-p": "topol.top"},
        output_dir=prep_output_dir,
    )
    mdp_json_em = get_file.override(task_id="get_min_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="min.json"
    )
    mdp_em = update_write_mdp_json_as_mdp_from_file(mdp_json_file_path=mdp_json_em)
    grompp_em = run_gmxapi.override(task_id="grompp_em")(
        args=["grompp"],
        input_files={"-f": mdp_em, "-c": solvate["-o"], "-p": solvate["-p"]},
        output_files={"-o": "em.tpr"},
        output_dir=prep_output_dir,
    )
    mdrun_em = run_gmxapi.override(task_id="mdrun_em")(
        args=["mdrun", "-v", "-deffnm", "em"],
        input_files={"-s": grompp_em["-o"]},
        output_files={"-c": "em.gro"},
        output_dir=prep_output_dir,
    )
    mdp_json_nvt = get_file.override(task_id="get_nvt_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="nvt.json"
    )
    mdp_nvt = update_write_mdp_json_as_mdp_from_file(mdp_json_file_path=mdp_json_nvt)
    grompp_nvt = run_gmxapi.override(task_id="grompp_nvt")(
        args=["grompp"],
        input_files={
            "-f": mdp_nvt,
            "-c": mdrun_em["-c"],
            "-r": mdrun_em["-c"],
            "-p": solvate["-p"],
        },
        output_files={"-o": "nvt.tpr"},
        output_dir=prep_output_dir,
    )
    mdrun_nvt = run_gmxapi.override(task_id="mdrun_nvt")(
        args=["mdrun", "-v", "-deffnm", "nvt"],
        input_files={"-s": grompp_nvt["-o"]},
        output_files={"-c": "nvt.gro"},
        output_dir=prep_output_dir,
    )


@task
def extract_edr_info(edr_file, field):
    import pyedr

    edr = pyedr.edr_to_dict(edr_file)
    return edr[field].tolist()


@task
def plot_histograms(data_list, labels, hatch_list, xlabel, title, output_file):
    import matplotlib.pyplot as plt

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


with DAG(
    dag_id="equilibrate",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as equilibrate:
    gro_equil = get_file.override(task_id="get_equil_gro")(
        input_dir="prep", file_name="nvt.gro", use_ref_data=False
    )
    top_equil = get_file.override(task_id="get_equil_top")(
        input_dir="prep", file_name="topol.top", use_ref_data=False
    )
    mdp_json_equil = get_file.override(task_id="get_equil_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="equil.json"
    )
    mdp_equil = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=mdp_json_equil
    ).expand(
        update_dict=[{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}]
    )
    equil_output_dir = "equil"
    grompp_input_list_equil = prepare_gmxapi_input(
        args=["grompp"],
        input_files={
            "-f": mdp_equil,
            "-c": gro_equil,
            "-r": gro_equil,
            "-p": top_equil,
        },
        output_files={"-o": "equil.tpr"},
        output_dir=equil_output_dir,
        counter=0,
        num_simulations=NUM_SIMULATIONS,
    )
    grompp_equil = run_gmxapi_dataclass.override(task_id="grompp_equil").expand(
        input_data=grompp_input_list_equil
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare_equil")
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
        .expand(gmxapi_output=grompp_equil)
    )
    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun_equil").expand(
        input_data=mdrun_input
    )
    edr_files = mdrun_result.map(lambda x: x["outputs"]["-e"])
    potential_energies_list_equil = (
        extract_edr_info.override(task_id="gmx_ener_equil")
        .partial(field="Potential")
        .expand(edr_file=edr_files)
    )
    hist_equil = plot_histograms.override(task_id="plot_histograms_equil")(
        data_list=potential_energies_list_equil,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="potential_energy_histogram.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )


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


with DAG(
    dag_id="simulate",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as simulate:
    import gmxapi, os

    gro_sim = (
        get_file.override(task_id="get_sim_gro")
        .partial(file_name="result.gro", use_ref_data=False)
        .expand(input_dir=[f"equil/step_0/sim_{i}" for i in range(NUM_SIMULATIONS)])
    )
    top_sim = get_file.override(task_id="get_sim_top")(
        input_dir="prep", file_name="topol.top", use_ref_data=False
    )
    cpt_sim = (
        get_file.override(task_id="get_sim_cpt")
        .partial(file_name="result.cpt", use_ref_data=False)
        .expand(input_dir=[f"equil/step_0/sim_{i}" for i in range(NUM_SIMULATIONS)])
    )
    mdp_json_sim = get_file.override(task_id="get_sim_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="sim.json"
    )
    mdp_sim = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=mdp_json_sim
    ).expand(
        update_dict=[{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}]
    )
    sim_output_dir = "sim"
    grompp_input_list_sim = prepare_gmxapi_input(
        args=["grompp"],
        input_files={
            "-f": mdp_sim,
            "-c": gro_sim,
            "-r": gro_sim,
            "-p": top_sim,
            "-t": cpt_sim,
        },
        output_files={"-o": "sim.tpr"},
        output_dir=sim_output_dir,
        counter=0,
        num_simulations=NUM_SIMULATIONS,
    )
    grompp_sim = run_gmxapi_dataclass.override(task_id="grompp_sim").expand(
        input_data=grompp_input_list_sim
    )
    mdrun_sim = BashOperator(
        bash_command=f"mpirun -np 4 {gmxapi.commandline.cli_executable()} mdrun -replex 100 -multidir sim/step_0/sim_[0123] -s sim.tpr",
        task_id="mdrun_sim",
        cwd=os.path.curdir,
    )
    potential_energy_list_sim = (
        extract_edr_info.override(task_id="gmx_ener_sim")
        .partial(field="Potential")
        .expand(
            edr_file=[f"sim/step_0/sim_{i}/ener.edr" for i in range(NUM_SIMULATIONS)]
        )
    )
    hist_sim = plot_histograms.override(task_id="plot_histograms_sim")(
        data_list=potential_energy_list_sim,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="potential_energy_histogram_sim.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )
    get_final_tpr = get_file.override(task_id="get_final_tpr")(
        input_dir="sim/step_0/sim_0", file_name="sim.tpr", use_ref_data=False
    )
    get_final_xtc = get_file.override(task_id="get_final_xtc")(
        input_dir="sim/step_0/sim_0", file_name="traj_comp.xtc", use_ref_data=False
    )
    plot_ramachandran = plot_ramachandran_residue.override(task_id="plot_ramachandran")(
        tpr_file=get_final_tpr,
        xtc_file=get_final_xtc,
        resnum=2,
        output_file="ramaPhiPsiALA2.png",
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
) as coordinate:
    is_setup_done = get_file.override(task_id="is_setup_done")(
        input_dir="prep",
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
        .expand(input_dir=[f"equil/step_0/sim_{i}" for i in range(NUM_SIMULATIONS)])
    )
    trigger_equil = TriggerDagRunOperator(
        task_id="trigger_equil",
        trigger_dag_id="equilibrate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
    )
    equil_done = EmptyOperator(task_id="equil_done", trigger_rule="none_failed")
    equil_done_branch = branch_task.override(task_id="equil_done_branch")(
        truth_value=is_equil_done,
        task_if_true=equil_done.task_id,
        task_if_false=trigger_equil.task_id,
    )

    is_sim_done = get_file.override(task_id="is_sim_done")(
        input_dir="sim/step_0/sim_0",
        file_name="sim.tpr",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_sim = TriggerDagRunOperator(
        task_id="trigger_sim",
        trigger_dag_id="simulate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
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
