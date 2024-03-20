from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "nvt_equil",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
        {
            "mdp": {"directory": "mdp", "filename": "nvt.json"},
            "gro": {"directory": "em", "filename": "em.gro"},
            "top": {"directory": "prep", "filename": "topol.top"},
         },

            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
                "properties": {"mdp": {"type": ["object", "null"]}, "gro": {"type": ["object", "null"]}, "top": {"type": ["object", "null"]} },
                "required": ["mdp", "gro", "top"],
            },
            section="inputs",
        ),
        "output_dir": "nvt_equil",
    },
) as nvt_equil:
    nvt_equil.doc = """Generic gromacs setup."""

    mdp_json_nvt = get_file.override(task_id="get_nvt_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}", file_name="{{ params.inputs.mdp.filename }}"
    )
    mdp_nvt = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_nvt_update")(mdp_json_file_path=mdp_json_nvt)
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}", file_name="{{ params.inputs.top.filename }}", use_ref_data=False
    )
    gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}", file_name="{{ params.inputs.gro.filename }}", use_ref_data=False
    )
    grompp_nvt = run_gmxapi.override(task_id="grompp_nvt")(
        args=["grompp"],
        input_files={
            "-f": mdp_nvt,
            "-c": gro,
            "-r": gro,
            "-p": top,
        },
        output_files={"-o": "nvt.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_nvt = run_gmxapi.override(task_id="mdrun_nvt")(
        args=["mdrun", "-v", "-deffnm", "nvt"],
        input_files={"-s": grompp_nvt["-o"]},
        output_files={"-c": "nvt.gro"},
        output_dir="{{ params.output_dir }}",
    )
