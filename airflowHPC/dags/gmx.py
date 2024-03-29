from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils import timezone

from airflowHPC.dags.tasks import get_file, run_gmxapi


@task
def protein_com_hdf5(input_files, output_files):
    import MDAnalysis as mda
    import h5py, os

    u = mda.Universe(input_files["gro"], input_files["xtc"])
    prot = u.select_atoms("protein")
    prot_com = prot.center_of_mass()
    if not os.path.exists(output_files["output_dir"]):
        os.makedirs(output_files["output_dir"])

    output_file = os.path.join(output_files["output_dir"], output_files["hdf5"])
    with h5py.File(output_file, "w") as f:
        f.create_dataset("protein_com", data=prot_com)

    return output_file


@task
def protein_cog_hdf5(input_files, output_files):
    import MDAnalysis as mda
    import h5py, os

    u = mda.Universe(input_files["gro"], input_files["xtc"])
    prot = u.select_atoms("protein")
    prot_cog = prot.center_of_geometry()
    if not os.path.exists(output_files["output_dir"]):
        os.makedirs(output_files["output_dir"])

    output_file = os.path.join(output_files["output_dir"], output_files["hdf5"])
    with h5py.File(output_file, "w") as f:
        f.create_dataset("protein_cog", data=prot_cog)

    return output_file


@task
def protein_mass(input_files):
    import MDAnalysis as mda

    u = mda.Universe(input_files["gro"], input_files["xtc"])
    prot = u.select_atoms("protein")
    prot_mass = prot.total_mass()

    return prot_mass


@task.branch
def decide_calculation(prot_mass):
    if prot_mass > 150:
        return "protein_com_hdf5"
    else:
        return "protein_cog_hdf5"


@task_group
def analyze(mdrun_result):
    protein_mass_result = protein_mass(
        {"gro": mdrun_result["-c"], "xtc": mdrun_result["-x"]}
    )
    calc_decider = decide_calculation(protein_mass_result)

    com = protein_com_hdf5(
        {"gro": mdrun_result["-c"], "xtc": mdrun_result["-x"]},
        {"hdf5": "com.hdf5", "output_dir": "outputs"},
    )
    cog = protein_cog_hdf5(
        {"gro": mdrun_result["-c"], "xtc": mdrun_result["-x"]},
        {"hdf5": "cog.hdf5", "output_dir": "outputs"},
    )
    calc_decider.set_downstream(com)
    calc_decider.set_downstream(cog)


with DAG("run_gmxapi", start_date=timezone.utcnow(), catchup=False) as dag:
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    input_top = get_file.override(task_id="get_top")(
        input_dir="ensemble_md", file_name="sys.top"
    )
    input_mdp = get_file.override(task_id="get_mdp")(
        input_dir="ensemble_md", file_name="expanded.mdp"
    )
    output_dir = "outputs"
    grompp_result = run_gmxapi.override(task_id="grompp")(
        args=["grompp"],
        input_files={"-f": input_mdp, "-c": input_gro, "-p": input_top},
        output_files={"-o": "run.tpr"},
        output_dir=output_dir,
    )
    mdrun_result = run_gmxapi.override(task_id="mdrun")(
        args=["mdrun"],
        input_files={"-s": grompp_result["-o"]},
        output_files={"-c": "result.gro", "-x": "result.xtc"},
        output_dir=output_dir,
    )
    # analyze(mdrun_result)
