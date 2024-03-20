from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi_dataclass, prepare_gmxapi_input, update_gmxapi_input
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    dag_id="equilibrate",
    start_date=timezone.utcnow(),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"input_dir": "prep", "mdp_input_dir": "ala_tripeptide_remd", "output_dir": "equil", "ref_t_list": [300, 310, 320, 330]},
) as equilibrate:
    equilibrate.doc = """Equilibration of a system for replica exchange."""

    gro_equil = get_file.override(task_id="get_equil_gro")(
        input_dir="{{ params.input_dir }}", file_name="nvt.gro", use_ref_data=False
    )
    top_equil = get_file.override(task_id="get_equil_top")(
        input_dir="{{ params.input_dir }}", file_name="topol.top", use_ref_data=False
    )
    mdp_json_equil = get_file.override(task_id="get_equil_mdp_json")(
        input_dir="{{ params.mdp_input_dir }}", file_name="equil.json"
    )
    mdp_equil = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_equil_update").partial(
        mdp_json_file_path=mdp_json_equil
    ).expand(
        update_dict=[{"ref_t": ref_t} for ref_t in equilibrate.params.get_param("ref_t_list").value]
    )
    grompp_input_list_equil = prepare_gmxapi_input(
        args=["grompp"],
        input_files={
            "-f": mdp_equil,
            "-c": gro_equil,
            "-r": gro_equil,
            "-p": top_equil,
        },
        output_files={"-o": "equil.tpr"},
        output_dir="{{ params.output_dir }}",
        counter=0,
        num_simulations=len(equilibrate.params.get_param("ref_t_list").value),
    )
    grompp_equil = run_gmxapi_dataclass.override(task_id="grompp_equil").expand(
        input_data=grompp_input_list_equil
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare_equil")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-e": "ener.edr",
                "-c": "result.gro",
                "-x": "result.xtc",
                "-cpo": "result.cpt",
            },
        )
        .expand(gmxapi_output=grompp_equil)
    )
    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun_equil").expand(
        input_data=mdrun_input
    )