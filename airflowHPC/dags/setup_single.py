import os
from airflow import DAG
from airflow.decorators import task
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
    dag_id="setup_single",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "output_dir": "setup_single",
        "setup": {
            "inputs": {
                "pdb": {
                    "directory": "ala_pentapeptide",
                    "filename": "ala_capped_pentapeptide.pdb",
                }
            },
            "box_size": 3.2,
            "force_field": "amber99sb-ildn",
            "water_model": "tip3p",
            "ion_concentration": 0,
            "expected_output": "system_prepared",
            "output_dir": "prep",
        },
        "minimize": {
            "inputs": {
                "mdp": {"directory": "mdp", "filename": "min.json"},
            },
            "expected_output": "em",
            "output_dir": "em",
            "mdp_updates": {},
        },
        "nvt": {
            "inputs": {
                "mdp": {"directory": "mdp", "filename": "nvt.json"},
            },
            "expected_output": "nvt",
            "output_dir": "nvt_equil",
            "mdp_updates": {
                "include": "-Isetup_single/prep/posre.itp",
                "define": "-DPOSRES",
            },
        },
        "npt": {
            "inputs": {
                "mdp": {"directory": "mdp", "filename": "npt.json"},
            },
            "expected_output": "npt",
            "output_dir": "npt_equil",
            "mdp_updates": {
                "include": "-Isetup_single/prep/posre.itp",
                "define": "-DPOSRES",
            },
        },
        "sim": {
            "inputs": {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
            },
            "expected_output": "sim",
            "output_dir": "sim",
            "mdp_updates": {
                "comm-grps": ["Protein", "Non-Protein"],
                "nsteps": 5000,
            },
        },
    },
) as setup_single:
    setup_single.doc = """Set up and equilibrate a system."""

    setup_params = {
        "inputs": "{{ params.setup.inputs }}",
        "output_dir": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
        "box_size": "{{ params.setup.box_size }}",
        "force_field": "{{ params.setup.force_field }}",
        "water_model": "{{ params.setup.water_model }}",
        "ion_concentration": "{{ params.setup.ion_concentration }}",
        "expected_output": "{{ params.setup.expected_output }}.gro",
    }
    setup = run_if_needed.override(group_id="prepare_system")(
        dag_id="prepare_system", dag_params=setup_params
    )

    minimize_params = {
        "inputs": {
            "mdp": "{{ params.minimize.inputs.mdp }}",
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
                "filename": "{{ params.setup.expected_output }}.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
                "filename": "{{ params.setup.expected_output }}.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.minimize.output_dir }}",
        "expected_output": "{{ params.minimize.expected_output }}.gro",
        "mdp_updates": "{{ params.minimize.mdp_updates }}",
    }
    minimize = run_if_needed.override(group_id="minimize")(
        dag_id="simulate_no_cpt",
        dag_params=minimize_params,
        dag_display_name="minimize",
    )

    nvt_params = {
        "inputs": {
            "mdp": "{{ params.nvt.inputs.mdp }}",
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.minimize.output_dir }}",
                "filename": "{{ params.minimize.expected_output }}.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
                "filename": "{{ params.setup.expected_output }}.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.nvt.output_dir }}",
        "expected_output": "{{ params.nvt.expected_output }}.gro",
        "mdp_updates": "{{ params.nvt.mdp_updates }}",
    }
    nvt_equil = run_if_needed.override(group_id="nvt_equil")(
        dag_id="simulate_no_cpt", dag_params=nvt_params, dag_display_name="nvt_equil"
    )

    npt_params = {
        "inputs": {
            "mdp": "{{ params.npt.inputs.mdp }}",
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.nvt.output_dir }}",
                "filename": "{{ params.nvt.expected_output }}.gro",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
                "filename": "{{ params.setup.expected_output }}.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.npt.output_dir }}",
        "expected_output": "{{ params.npt.expected_output }}.gro",
        "mdp_updates": "{{ params.npt.mdp_updates }}",
    }
    npt_equil_has_run = verify_files.override(task_id="npt_equil_has_run")(
        input_dir="{{ params.output_dir }}/{{ params.npt.output_dir }}",
        filename="{{ params.npt.expected_output }}.gro",
    )
    npt_equil = run_if_false.override(group_id="npt_equil")(
        dag_id="simulate_no_cpt",
        dag_params=npt_params,
        truth_value=npt_equil_has_run,
        dag_display_name="npt_equil",
    )

    sim_params = {
        "inputs": {
            "mdp": "{{ params.sim.inputs.mdp }}",
            "gro": {
                "directory": "{{ params.output_dir }}/{{ params.npt.output_dir }}",
                "filename": "{{ params.npt.expected_output }}.gro",
            },
            "cpt": {
                "directory": "{{ params.output_dir }}/{{ params.npt.output_dir }}",
                "filename": "{{ params.npt.expected_output }}.cpt",
            },
            "top": {
                "directory": "{{ params.output_dir }}/{{ params.setup.output_dir }}",
                "filename": "{{ params.setup.expected_output }}.top",
            },
        },
        "output_dir": "{{ params.output_dir }}/{{ params.sim.output_dir }}",
        "expected_output": "{{ params.sim.expected_output }}.gro",
        "mdp_updates": "{{ params.sim.mdp_updates }}",
    }
    sim_has_run = verify_files.override(task_id="sim_has_run")(
        input_dir="{{ params.output_dir }}/{{ params.sim.output_dir }}",
        filename="{{ params.sim.expected_output }}.gro",
    )
    simulate = run_if_false.override(group_id="simulate")(
        dag_id="simulate_cpt",
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
