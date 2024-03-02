from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflowHPC.dags.tasks import (
    get_file,
    run_gmxapi,
    branch_task,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
)

start_date = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    "system_setup",
    start_date=start_date,
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
    mdp_em = get_file.override(task_id="get_min_mdp")(
        input_dir="ala_tripeptide_remd", file_name="min.mdp"
    )
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
    mdp_nvt = get_file.override(task_id="get_nvt_mdp")(
        input_dir="ala_tripeptide_remd", file_name="nvt.mdp"
    )
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
def forward_values(values):
    return list(values)


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
    start_date=start_date,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as equilibrate:
    gro_equil = get_file.override(task_id="get_equil_gro")(
        input_dir="prep", file_name="nvt.gro", use_ref_data=False
    )
    top = get_file.override(task_id="get_equil_top")(
        input_dir="prep", file_name="topol.top", use_ref_data=False
    )
    mdp_json = get_file.override(task_id="get_equil_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="equil.json"
    )
    mdp_equil = update_write_mdp_json_as_mdp_from_file.partial(
        mdp_json_file_path=mdp_json
    ).expand(
        update_dict=[{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}]
    )
    mdp_list = forward_values(mdp_equil)
    equil_output_dir = "equil"
    grompp_input_list = prepare_gmxapi_input(
        args=["grompp"],
        input_files={"-f": mdp_list, "-c": gro_equil, "-r": gro_equil, "-p": top},
        output_files={"-o": "equil.tpr"},
        output_dir=equil_output_dir,
        counter=0,
        num_simulations=4,
    )
    grompp_equil = run_gmxapi_dataclass.override(task_id="grompp_equil").expand(
        input_data=grompp_input_list
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare_equil")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={"-e": "ener.edr", "-c": "result.gro", "-x": "result.xtc"},
        )
        .expand(gmxapi_output=grompp_equil)
    )
    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun_equil").expand(
        input_data=mdrun_input
    )
    edr_files = mdrun_result.map(lambda x: x["outputs"]["-e"])
    potential_energies_list = extract_edr_info.partial(field="Potential").expand(
        edr_file=edr_files
    )
    plot_histograms(
        data_list=potential_energies_list,
        labels=["300K", "310K", "320K", "330K"],
        hatch_list=["/", ".", "\\", "o"],
        output_file="potential_energy_histogram.png",
        xlabel="Potential Energy (kJ/mol)",
        title="Potential Energy Histogram",
    )


with DAG(
    dag_id="coordinate",
    start_date=start_date,
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
    is_equil_done = get_file.override(task_id="is_equil_done")(
        input_dir="equil",
        file_name="equil.tpr",
        use_ref_data=False,
        check_exists=True,
    )
    trigger_equil = TriggerDagRunOperator(
        task_id="trigger_equil",
        trigger_dag_id="equilibrate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
    )
    equil_done = EmptyOperator(task_id="equil_done", trigger_rule="none_failed")

    setup_done_branch = branch_task.override(task_id="setup_done_branch")(
        is_setup_done, setup_done.task_id, trigger_setup.task_id
    )
    equil_done_branch = branch_task.override(task_id="equil_done_branch")(
        is_equil_done, equil_done.task_id, trigger_equil.task_id
    )

    is_setup_done >> setup_done_branch >> [trigger_setup, setup_done] >> is_equil_done
    is_equil_done >> equil_done_branch >> [trigger_equil, equil_done]
