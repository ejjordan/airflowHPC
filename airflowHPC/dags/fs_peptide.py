import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflowHPC.dags.tasks import run_if_needed


@task.short_circuit(trigger_rule="none_failed")
def verify_files(input_dir, filename, ref_t_list, step_number):
    """Workaround for simulation step where multiple files are expected."""
    import logging

    input_files = [
        f"{input_dir}/step_{step_number}/sim_{i}/{filename}"
        for i in range(len(ref_t_list))
    ]
    for file in input_files:
        logging.info(f"Checking if {file} exists: {os.path.exists(file)}")
        if not os.path.exists(file):
            return True
    return False


with DAG(
    dag_id="fs_peptide",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"ref_t_list": [300, 310, 320, 330]},
) as fs_peptide:
    fs_peptide.doc = """Replica exchange simulation of a peptide in water."""

    setup_params = {
        "inputs": {"pdb": {"directory": "fs_peptide", "filename": "fs.pdb"}},
        "output_dir": "prep",
        "box_size": 4,
        "force_field": "amber99sb-ildn",
        "water_model": "tip3p",
        "ion_concentration": 0.15,
        "expected_output": "system_prepared.gro",
    }
    setup = run_if_needed.override(group_id="prepare_system")(
        "prepare_system", setup_params
    )

    minimize_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "min.json"},
            "gro": {"directory": "prep", "filename": "system_prepared.gro"},
            "top": {"directory": "prep", "filename": "topol.top"},
        },
        "output_dir": "em",
        "expected_output": "em.gro",
    }
    minimize = run_if_needed.override(group_id="minimize")("minimize", minimize_params)

    nvt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "nvt.json"},
            "gro": {"directory": "em", "filename": "em.gro"},
            "top": {"directory": "prep", "filename": "topol.top"},
        },
        "output_dir": "nvt_equil",
        "expected_output": "nvt.gro",
    }
    nvt_equil = run_if_needed.override(group_id="nvt_equil")("nvt_equil", nvt_params)

    npt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "npt.json"},
            "gro": {"directory": "nvt_equil", "filename": "nvt.gro"},
            "top": {"directory": "prep", "filename": "topol.top"},
        },
        "ref_t_list": "{{ params.ref_t_list }}",
        "step_number": 0,
        "output_dir": "npt_equil",
        "expected_output": "npt.gro",
    }
    npt_equil = run_if_needed.override(group_id="npt_equil")("npt_equil", npt_params)

    sim_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "sim.json"},
            "gro": {"directory": "npt_equil", "filename": "npt.gro"},
            "cpt": {"directory": "npt_equil", "filename": "npt.cpt"},
            "top": {"directory": "prep", "filename": "topol.top"},
        },
        "ref_t_list": "{{ params.ref_t_list }}",
        "step_number": 0,
        "output_dir": "sim",
        "expected_output": "sim.gro",
    }
    verify_files = verify_files.override(task_id="check_sim_done")(
        input_dir="sim",
        filename="sim.gro",
        ref_t_list="{{ params.ref_t_list }}",
        step_number=0,
    )
    simulate = TriggerDagRunOperator(
        task_id=f"trigger_simulate",
        trigger_dag_id="simulate",
        wait_for_completion=True,
        poke_interval=10,
        trigger_rule="none_failed",
        params=sim_params,
    )

    setup >> minimize >> nvt_equil >> npt_equil >> verify_files >> simulate
