import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflowHPC.dags.tasks import run_if_needed, run_if_false


@task(trigger_rule="none_failed")
def verify_files(input_dir, filename):
    """Workaround for steps where multiple files are expected."""
    import logging

    input_file = f"{input_dir}/{filename}"

    logging.info(f"Checking if {input_file} exists: {os.path.exists(input_file)}")
    if not os.path.exists(input_file):
        return False
    return True


with DAG(
    dag_id="rct_setup_single",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"output_dir": "setup_single"},
) as setup_single:
    setup_single.doc = """Set up and equilibrate a system."""

    setup_params = {
        "inputs": {
            "pdb": {"directory": "ala_pentapeptide", "filename": "ala_pentapeptide.pdb"}
        },
        "output_dir": "{{ params.output_dir }}/prep",
        "box_size": 3.2,
        "force_field": "amber99sb-ildn",
        "water_model": "tip3p",
        "ion_concentration": 0.15,
        "expected_output": "system_prepared.gro",
    }
    setup = run_if_needed.override(group_id="prepare_system")(
        dag_id="rct_prepare_system", dag_params=setup_params
    )

    minimize_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "min.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/prep",
                "filename": "system_prepared.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/prep",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/em",
        "expected_output": "em.gro",
    }
    minimize = run_if_needed.override(group_id="minimize")(
        dag_id="rct_simulate_no_cpt",
        dag_params=minimize_params,
        dag_display_name="minimize",
    )

    nvt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "nvt.json"},
            "gro": {"directory": "{{ params.output_dir }}/em", "filename": "em.gro"},
            "top": {
                "directory": "{{ params.output_dir }}/prep",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/nvt_equil",
        "expected_output": "nvt.gro",
    }
    nvt_equil = run_if_needed.override(group_id="nvt_equil")(
        dag_id="rct_simulate_no_cpt", dag_params=nvt_params, dag_display_name="nvt_equil"
    )

    npt_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "npt.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/nvt_equil",
                "filename": "nvt.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/prep",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/npt_equil",
        "expected_output": "npt.gro",
    }
    npt_equil_has_run = verify_files.override(task_id="npt_equil_has_run")(
        input_dir="{{ params.output_dir }}/npt_equil",
        filename="npt.gro",
    )
    npt_equil = run_if_false.override(group_id="npt_equil")(
        dag_id="rct_simulate_no_cpt",
        dag_params=npt_params,
        truth_value=npt_equil_has_run,
        dag_display_name="npt_equil",
    )

    sim_params = {
        "inputs": {
            "mdp": {"directory": "mdp", "filename": "sim.json"},
            "gro": {
                "directory": "{{ params.output_dir }}/npt_equil",
                "filename": "npt.gro",
            },
            "cpt": {
                "directory": "{{ params.output_dir }}/npt_equil",
                "filename": "npt.cpt",
            },
            "top": {
                "directory": "{{ params.output_dir }}/prep",
                "filename": "system_prepared.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/sim",
        "expected_output": "sim.gro",
    }
    sim_has_run = verify_files.override(task_id="sim_has_run")(
        input_dir="{{ params.output_dir }}/sim",
        filename="sim.gro",
    )
    simulate = run_if_false.override(group_id="simulate")(
        dag_id="rct_simulate_cpt",
        dag_params=sim_params,
        truth_value=sim_has_run,
        dag_display_name="simulate",
    )

    (
        setup
        >> minimize
        >> nvt_equil
        >> npt_equil_has_run
        >> npt_equil
        >> sim_has_run
        >> simulate
    )
