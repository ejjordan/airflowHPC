from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
    list_from_xcom_dicts,
    json_from_dataset_path,
)
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


@task(multiple_outputs=True)
def get_all_files_in_dir(
    input_dir,
    use_ref_data: bool = True,
):
    import logging
    import os
    from airflowHPC.data import data_dir as data

    if use_ref_data:
        data_dir = os.path.abspath(os.path.join(data, input_dir))
    else:
        data_dir = os.path.abspath(input_dir)

    logging.info(f"Checking if {data_dir} exists: {os.path.exists(data_dir)}")
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Directory {data_dir} does not exist")
    files = sorted(os.listdir(data_dir))
    return {"-f": [os.path.join(data_dir, file) for file in files]}


with DAG(
    "alanine_dipeptide",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "pdb": {"directory": "ala_dipeptide_string"},
                "em_mdp": {"directory": "mdp", "filename": "min.json"},
                "sim_mdp": {"directory": "mdp", "filename": "sim.json"},
            },
            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
                "properties": {"pdb": {"type": ["object", "null"]}},
                "required": ["pdb"],
            },
            section="inputs",
        ),
        "output_dir": "ala_dipeptide",
        "box_size": 1,
        "force_field": "amber99sb-ildn",
        "water_model": "tip3p",
    },
) as dag:
    dag.doc = """Generic gromacs setup. If param ion_concentration is 0.0, genion is not run."""

    input_pdbs = get_all_files_in_dir.override(task_id="get_pdbs")(
        input_dir="{{ params.inputs.pdb.directory }}"
    )
    pdb2gmx_input_list = prepare_gmxapi_input.override(task_id="pdb2gmx_input_list")(
        args=[
            "pdb2gmx",
            "-ff",
            "{{ params.force_field }}",
            "-water",
            "{{ params.water_model }}",
        ],
        input_files=input_pdbs,
        output_files={"-o": "system_initial.gro", "-p": "topol.top", "-i": "posre.itp"},
        output_dir="{{ params.output_dir }}",
        counter=0,
        num_simulations="{{task_instance.xcom_pull(task_ids='get_pdbs', key='-f') | length}}",
    )
    pdb2gmx = run_gmxapi_dataclass.override(
        task_id="pdb2gmx", max_active_tis_per_dagrun=8
    ).expand(input_data=pdb2gmx_input_list)

    editconf_input_list = (
        update_gmxapi_input.override(task_id="editconf_input_list")
        .partial(
            args=[
                "editconf",
                "-bt",
                "dodeca",
                "-d",
                "{{ params.box_size }}",
            ],
            input_files_keys={"-f": "-o"},
            output_files={"-o": "system_box.gro"},
        )
        .expand(gmxapi_output=pdb2gmx)
    )
    editconf = run_gmxapi_dataclass.override(
        task_id="editconf", max_active_tis_per_dagrun=8
    ).expand(input_data=editconf_input_list)

    solvate_input_list = (
        update_gmxapi_input.override(task_id="solvate_input_list")
        .partial(
            args=["solvate", "-cs", "spc216.gro"],
            input_files_keys={"-cp": "-o"},
            output_files={"-o": "system_solv.gro", "-p": "topol.top"},
        )
        .expand(gmxapi_output=editconf)
    )
    # The call to solvate only works because the top file is in the execution directory
    solvate = run_gmxapi_dataclass.override(
        task_id="solvate", max_active_tis_per_dagrun=8
    ).expand(input_data=solvate_input_list)

    em_grompp_mdrun_params = {
        "mdp": {"directory": "mdp", "filename": "min.json"},
        "gro_task_id": "solvate",
        "top_task_id": "solvate",
        "parent_dag_id": dag.dag_id,
        "num_simulations": "{{task_instance.xcom_pull(task_ids='get_pdbs', key='-f') | length}}",
        "output_name": "em",
        "output_dir": "{{ params.output_dir }}",
    }
    em_dag = TriggerDagRunOperator(
        task_id="em_dag",
        trigger_dag_id="grompp_mdrun",
        # trigger_run_id="em_run",
        conf=em_grompp_mdrun_params,
        wait_for_completion=True,
    )
    solvate >> em_dag

    em_gros = json_from_dataset_path.override(task_id="em_gro_dataset")(
        dataset_path="{{ params.output_dir }}/em.json"
    )
    em_dag >> em_gros

    sim_grompp_mdrun_params = {
        "mdp": {"directory": "mdp", "filename": "sim.json"},
        "gro_task_id": "em_gros",
        "top_task_id": "solvate",
        "parent_dag_id": dag.dag_id,
        "num_simulations": "{{task_instance.xcom_pull(task_ids='get_pdbs', key='-f') | length}}",
        "output_name": "sim",
        "output_dir": "{{ params.output_dir }}",
    }
    sim_dag = TriggerDagRunOperator(
        task_id="sim_dag",
        trigger_dag_id="grompp_mdrun",
        # trigger_run_id="sim_run",
        conf=sim_grompp_mdrun_params,
        wait_for_completion=True,
    )
    em_gros >> sim_dag

    sim_gros = json_from_dataset_path.override(task_id="sim_gro_dataset")(
        dataset_path="{{ params.output_dir }}/sim.json"
    )
    sim_dag >> sim_gros
