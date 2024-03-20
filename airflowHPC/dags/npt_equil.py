from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmxapi_input,
    run_gmxapi_dataclass,
    update_gmxapi_input,
    unpack_ref_t,
)
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    "npt_equil",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "npt.json"},
                "gro": {"directory": "em", "filename": "em.gro"},
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
        "output_dir": "npt_equil",
        "expected_output": "npt.gro",
        "ref_t_list": [300, 310, 320, 330],
        "step_number": 0,
    },
) as npt_equil:
    npt_equil.doc = """NPT equilibration."""

    mdp_json_npt = get_file.override(task_id="get_npt_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    ref_temps = unpack_ref_t()
    mdp_npt = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_npt_update")
        .partial(mdp_json_file_path=mdp_json_npt)
        .expand(update_dict=ref_temps)
    )
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro = get_file.override(task_id="get_gro")(
        input_dir="{{ params.inputs.gro.directory }}",
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
    )
    grompp_input_list_npt = prepare_gmxapi_input(
        args=["grompp"],
        input_files={
            "-f": mdp_npt,
            "-c": gro,
            "-r": gro,
            "-p": top,
        },
        output_files={"-o": "npt.tpr"},
        output_dir="{{ params.output_dir }}",
        counter="{{ params.step_number }}",
        num_simulations="{{ params.ref_t_list | length }}",
    )
    grompp_npt = run_gmxapi_dataclass.override(task_id="grompp_npt").expand(
        input_data=grompp_input_list_npt
    )
    mdrun_input = (
        update_gmxapi_input.override(task_id="mdrun_prepare_npt")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-e": "ener.edr",
                "-c": "{{ params.expected_output }}",
                "-x": "npt.xtc",
                "-cpo": "npt.cpt",
            },
        )
        .expand(gmxapi_output=grompp_npt)
    )
    mdrun_result = run_gmxapi_dataclass.override(task_id="mdrun_npt").expand(
        input_data=mdrun_input
    )
