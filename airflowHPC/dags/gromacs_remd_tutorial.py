from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflowHPC.dags.tasks import get_file, run_gmxapi, branch_task


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


with DAG(
    dag_id="equilibrate",
    start_date=start_date,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as equilibrate:
    mdp_json = get_file.override(task_id="get_equil_mdp_json")(
        input_dir="ala_tripeptide_remd", file_name="equil.json"
    )
    mdp_equil = update_write_mdp_json_as_mdp_from_file(mdp_json, {})
    gro_equil = get_file.override(task_id="get_equil_gro")(
        input_dir="prep", file_name="nvt.gro", use_ref_data=False
    )
    top = get_file.override(task_id="get_equil_top")(
        input_dir="prep", file_name="topol.top", use_ref_data=False
    )
    equil_output_dir = "equil"
    grompp_equil = run_gmxapi.override(task_id="grompp_equil")(
        args=["grompp"],
        input_files={"-f": mdp_equil, "-c": gro_equil, "-r": gro_equil, "-p": top},
        output_files={"-o": "equil.tpr"},
        output_dir=equil_output_dir,
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
