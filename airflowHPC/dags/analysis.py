from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file


@task
def protein_mass(gro, selection: str = "protein"):
    import MDAnalysis as mda

    u = mda.Universe(gro)
    prot = u.select_atoms(selection)
    prot_mass = prot.total_mass()

    return prot_mass


with DAG("get_mass", start_date=timezone.utcnow(), catchup=False) as dag:
    dag.doc = "Simple DAG to demo use of MDAnalysis in a task."
    input_gro = get_file.override(task_id="get_gro")(
        input_dir="ensemble_md", file_name="sys.gro"
    )
    mass = protein_mass(
        input_gro,
        selection="all",
    )
