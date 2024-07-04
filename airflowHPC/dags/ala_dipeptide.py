from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
    list_from_xcom_dicts,
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
        "expected_output": "system_prepared.gro",
    },
) as prepare:
    prepare.doc = """Generic gromacs setup. If param ion_concentration is 0.0, genion is not run."""

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
    solvate_gros = list_from_xcom_dicts.override(task_id="solvate_gros")(
        "{{task_instance.xcom_pull(task_ids='solvate', key='outputs')}}", "-o"
    )
    solvate_tops = list_from_xcom_dicts.override(task_id="solvate_tops")(
        "{{task_instance.xcom_pull(task_ids='solvate', key='outputs')}}", "-p"
    )
    solvate >> solvate_tops
    solvate >> solvate_gros

    em_mdp_json = get_file.override(task_id="get_min_mdp_json")(
        input_dir="{{ params.inputs.em_mdp.directory }}",
        file_name="{{ params.inputs.em_mdp.filename }}",
    )
    em_mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_em_update")(
        mdp_json_file_path=em_mdp_json
    )

    em_grompp_input_list = prepare_gmxapi_input.override(
        task_id="grompp_input_list_em"
    )(
        args=["grompp"],
        input_files={"-f": em_mdp, "-c": solvate_gros, "-p": solvate_tops},
        output_files={"-o": "em.tpr"},
        output_dir="{{ params.output_dir }}",
        counter=0,
        num_simulations="{{task_instance.xcom_pull(task_ids='get_pdbs', key='-f') | length}}",
    )
    solvate_tops >> em_grompp_input_list
    solvate_gros >> em_grompp_input_list
    em_mdp >> em_grompp_input_list
    em_grompp = run_gmxapi_dataclass.override(
        task_id="grompp_em", max_active_tis_per_dagrun=8
    ).expand(input_data=em_grompp_input_list)
    em_grompp_input_list >> em_grompp

    em_mdrun_input_list = (
        update_gmxapi_input.override(task_id="em_mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", "em"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "em.gro"},
        )
        .expand(gmxapi_output=em_grompp)
    )
    em_mdrun = run_gmxapi_dataclass.override(
        task_id="mdrun_em", max_active_tis_per_dagrun=1
    ).expand(input_data=em_mdrun_input_list)
    em_gros = list_from_xcom_dicts.override(task_id="em_gros")(
        "{{task_instance.xcom_pull(task_ids='mdrun_em', key='outputs')}}", "-c"
    )
    em_mdrun >> em_gros

    sim_mdp_json = get_file.override(task_id="get_sim_mdp_json")(
        input_dir="{{ params.inputs.sim_mdp.directory }}",
        file_name="{{ params.inputs.sim_mdp.filename }}",
    )
    sim_mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp_sim")(
        mdp_json_file_path=sim_mdp_json
    )
    sim_grompp_input_list = prepare_gmxapi_input.override(
        task_id="grompp_input_list_sim"
    )(
        args=["grompp"],
        input_files={
            "-f": sim_mdp,
            "-c": em_gros,
            "-p": solvate_tops,
        },
        output_files={"-o": "sim.tpr"},
        output_dir="{{ params.output_dir }}",
        counter=0,
        num_simulations="{{task_instance.xcom_pull(task_ids='get_pdbs', key='-f') | length}}",
    )
    em_gros >> sim_grompp_input_list
    sim_grompp = run_gmxapi_dataclass.override(
        task_id="grompp_sim", max_active_tis_per_dagrun=8
    ).expand(input_data=sim_grompp_input_list)

    sim_mdrun_input_list = (
        update_gmxapi_input.override(task_id="sim_mdrun_input_list")
        .partial(
            args=["mdrun", "-v", "-deffnm", "sim"],
            input_files_keys={"-s": "-o"},
            output_files={"-c": "sim.gro"},
        )
        .expand(gmxapi_output=sim_grompp)
    )
    sim_mdrun = run_gmxapi_dataclass.override(
        task_id="mdrun_sim", max_active_tis_per_dagrun=1
    ).expand(input_data=sim_mdrun_input_list)
