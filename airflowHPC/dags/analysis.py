import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.data import data_dir


@task.radical
def protein_mass(gro, selection: str = "protein"):
    import MDAnalysis as mda

    u = mda.Universe(gro)
    prot = u.select_atoms(selection)
    prot_mass = prot.total_mass()

    return prot_mass


with DAG("get_mass", start_date=timezone.utcnow(), catchup=False) as dag:
    dag.doc = "Simple DAG that has args, kwargs and a return value to test radical_task"
    mass = protein_mass(
        os.path.join(os.path.abspath(os.path.join(data_dir, "ensemble_md")), "sys.gro"),
        selection="all",
    )
