from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor


@task(multiple_outputs=False)
def get_refdata_file(
    input_dir, file_name, use_ref_data: bool = True, check_exists: bool = False
):
    import os
    from airflowHPC.data import data_dir as data

    if use_ref_data:
        data_dir = os.path.abspath(os.path.join(data, input_dir))
    else:
        data_dir = os.path.abspath(input_dir)
    file_to_get = os.path.join(data_dir, file_name)
    if check_exists:
        if os.path.exists(file_to_get):
            return True
        else:
            return False
    assert os.path.exists(file_to_get)
    return file_to_get


@task(multiple_outputs=True)
def run_gmxapi(args, input_files, output_files, output_dir):
    import os
    import gmxapi
    import logging

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_files_paths = {
        f"{k}": f"{os.path.join(out_path, v)}" for k, v in output_files.items()
    }
    cwd = os.getcwd()
    os.chdir(out_path)
    gmx = gmxapi.commandline_operation(
        gmxapi.commandline.cli_executable(), args, input_files, output_files_paths
    )
    gmx.run()
    logging.info(gmx.output.stderr.result())
    logging.info(gmx.output.stdout.result())
    os.chdir(cwd)
    assert all(
        [os.path.exists(gmx.output.file[key].result()) for key in output_files.keys()]
    )
    return {f"{key}": f"{gmx.output.file[key].result()}" for key in output_files.keys()}


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

    input_pdb = get_refdata_file.override(task_id="get_pdb")(
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
    mdp_em = get_refdata_file.override(task_id="get_min_mdp")(
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
    mdp_nvt = get_refdata_file.override(task_id="get_nvt_mdp")(
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
    mdp_equil = get_refdata_file.override(task_id="get_equil_mdp")(
        input_dir="ala_tripeptide_remd", file_name="equil.mdp"
    )
    gro_equil = get_refdata_file.override(task_id="get_equil_gro")(
        input_dir="prep", file_name="nvt.gro", use_ref_data=False
    )
    top = get_refdata_file.override(task_id="get_equil_top")(
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
    setup_done = get_refdata_file.override(task_id="setup_done")(
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
    )
    trigger_equil = TriggerDagRunOperator(
        task_id="trigger_equil",
        trigger_dag_id="equilibrate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed_min_one_success",
    )

    @task.branch
    def setup_done_branch(setup_done: bool) -> str:
        if setup_done:
            return "trigger_equil"
        else:
            return "trigger_setup"

    setup_done >> setup_done_branch(setup_done) >> [trigger_setup, trigger_equil]
    trigger_setup >> trigger_equil
