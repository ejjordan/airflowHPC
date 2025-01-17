from airflow import DAG
from airflow.utils.timezone import datetime
from airflowHPC.dags.tasks import get_file
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file
from airflow.models.param import Param


with DAG(
    dag_id="simulate_cpt",
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
                "gro": {"directory": "npt_equil", "filename": "npt.gro"},
                "cpt": {"directory": "npt_equil", "filename": "npt.cpt"},
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
                "required": ["mdp", "gro", "cpt", "top"],
            },
            section="inputs",
        ),
        "output_dir": "sim",
        "expected_output": "sim.gro",
        "mdp_updates": {},
    },
) as simulate_cpt:
    simulate_cpt.doc = """Simulation from a cpt file."""

    top_cpt = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro_cpt = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.gro.directory }}",
    )
    cpt_cpt = get_file.override(task_id="get_cpt")(
        file_name="{{ params.inputs.cpt.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.cpt.directory }}",
    )
    mdp_json_cpt = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_cpt = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")(
        mdp_json_file_path=mdp_json_cpt,
        update_dict="{{ params.mdp_updates }}",
    )

    grompp_cpt = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={
            "-f": mdp_cpt,
            "-c": gro_cpt,
            "-r": gro_cpt,
            "-p": top_cpt,
            "-t": cpt_cpt,
        },
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_cpt = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-e": "ener.edr",
            "-c": "{{ params.expected_output }}",
            "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
            "-cpo": "{{ params.expected_output | replace('.gro', '.cpt') }}",
        },
        output_dir="{{ params.output_dir }}",
    )
    grompp_cpt >> mdrun_cpt


with DAG(
    dag_id="simulate_no_cpt",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "sim.json"},
                "gro": {"directory": "npt_equil", "filename": "npt.gro"},
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
        "mdp_updates": {},
        "output_dir": "sim",
        "expected_output": "sim.gro",
    },
) as simulate_no_cpt:
    simulate_no_cpt.doc = """Simulation without a cpt file."""

    top_no_cpt = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data=False,
    )
    gro_no_cpt = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        use_ref_data=False,
        input_dir="{{ params.inputs.gro.directory }}",
    )
    mdp_json_no_cpt = get_file.override(task_id="get_mdp_json")(
        input_dir="{{ params.inputs.mdp.directory }}",
        file_name="{{ params.inputs.mdp.filename }}",
    )
    mdp_no_cpt = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")(
        mdp_json_file_path=mdp_json_no_cpt,
        update_dict="{{ params.mdp_updates }}",
    )

    grompp_no_cpt = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["grompp"],
        input_files={
            "-f": mdp_no_cpt,
            "-c": gro_no_cpt,
            "-r": gro_no_cpt,
            "-p": top_no_cpt,
        },
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_no_cpt = ResourceGmxOperator(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 4,
            "cpus_per_task": 2,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-e": "ener.edr",
            "-c": "{{ params.expected_output }}",
            "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
            "-cpo": "{{ params.expected_output | replace('.gro', '.cpt') }}",
        },
        output_dir="{{ params.output_dir }}",
    )
    grompp_no_cpt >> mdrun_no_cpt
