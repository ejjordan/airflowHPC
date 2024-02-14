from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone


@task(multiple_outputs=False)
def get_refdata_file(input_dir, file_name):
    import os
    from airflowHPC.data import data_dir

    return os.path.join(os.path.abspath(os.path.join(data_dir, input_dir)), file_name)


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


with DAG(
    "gromacs_remd_tutorial",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
) as dag:
    dag.doc = """Reworking of gromacs tutorial for replica exchange MD.
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
