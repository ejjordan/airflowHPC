from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "minimize",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "min.json"},
                "gro": {"directory": "prep", "filename": "system_prepared.gro"},
                "top": {"directory": "prep", "filename": "topol.top"},
            },
            type=["object", "null"],
            title="Inputs list",
            items={
                "type": "object",
                "properties": {
                    "mdp": {"type": ["object", "null"]},
                    "gro": {"type": ["object", "null"]},
                    "top": {"type": ["object", "null"]},
                },
                "required": ["mdp", "gro", "top"],
            },
            section="inputs",
        ),
        "output_dir": "em",
        "expected_output": "em.gro",
    },
) as minimize:
    minimize.doc = """Energy minimization."""

    mdp_json_em = get_file.override(task_id="get_min_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_em = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_em_update")(
        mdp_json_file_path=mdp_json_em
    )
    gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
    )
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    grompp_em = run_gmxapi.override(task_id="grompp_em")(
        args=["grompp"],
        input_files={"-f": mdp_em, "-c": gro, "-p": top},
        output_files={"-o": "em.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_em = run_gmxapi.override(task_id="mdrun_em")(
        args=["mdrun", "-v", "-deffnm", "em"],
        input_files={"-s": grompp_em["-o"]},
        output_files={"-c": "{{ params.expected_output }}"},
        output_dir="{{ params.output_dir }}",
    )
