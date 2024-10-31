from airflow import DAG
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    prepare_gmx_input,
    update_gmx_input,
    unpack_mdp_options,
    dataset_from_xcom_dicts,
)
from airflowHPC.operators import ResourceGmxOperatorDataclass
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


with DAG(
    dag_id="simulate_expand",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "npt.json"},
                "gro": {"directory": "nvt_equil", "filename": "nvt.gro"},
                "top": {"directory": "prep", "filename": "system_prepared.top"},
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
        "mdp_options": [{"ref_t": 300}, {"ref_t": 310}, {"ref_t": 320}, {"ref_t": 330}],
        "output_dir": "npt_equil",
        "expected_output": "npt.gro",
        "output_dataset_structure": {},
    },
) as simulate_multi:
    simulate_multi.doc = """Expand the simulation to multiple replicas."""

    mdp_json = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_options = unpack_mdp_options()
    mdp = (
        update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")
        .partial(mdp_json_file_path=mdp_json)
        .expand(update_dict=mdp_options)
    )
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        input_dir="{{ params.inputs.gro.directory }}",
        use_ref_data=False,
    )
    grompp_input_list = prepare_gmx_input.override(task_id="grompp_input_list")(
        args=["grompp"],
        input_files={"-f": mdp, "-c": gro, "-p": top},
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_path_parts=[
            "{{ params.output_dir }}",
            "sim_",
        ],
        num_simulations="{{ params.mdp_options | length }}",
    )

    grompp = ResourceGmxOperatorDataclass.partial(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=grompp_input_list)
    grompp_input_list >> grompp

    mdrun_input_list = (
        update_gmx_input.override(task_id="mdrun_input_list")
        .partial(
            args=["mdrun"],
            input_files_keys={"-s": "-o"},
            output_files={
                "-e": "ener.edr",
                "-c": "{{ params.expected_output }}",
                "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
                "-cpo": "{{ params.expected_output | replace('.gro', '.cpt') }}",
            },
        )
        .expand(gmx_output=grompp.output)
    )
    mdrun = ResourceGmxOperatorDataclass.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 2,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
    ).expand(input_data=mdrun_input_list)
    grompp >> mdrun_input_list >> mdrun

    dataset_dict = dataset_from_xcom_dicts.override(task_id="make_dataset")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.expected_output | replace('.gro', '.json') }}",
        list_of_dicts="{{task_instance.xcom_pull(task_ids='mdrun', key='return_value')}}",
        dataset_structure="{{ params.output_dataset_structure }}",
    )
    mdrun >> dataset_dict
