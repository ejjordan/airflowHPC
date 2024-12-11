from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import (
    get_file,
    dataset_from_xcom_dicts,
)
from airflowHPC.operators import ResourceGmxOperator
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file


@task
def outputs_list(**context):
    num_sims = int(context["task"].render_template("{{ params.num_sims }}", context))
    output_dir = context["task"].render_template("{{ params.output_dir }}", context)
    return [f"{output_dir}/sim_{i}" for i in range(num_sims)]


@task
def prepare_output(mdrun, grompp):
    mdrun = [md for md in mdrun]
    output = []
    for i, data in enumerate(mdrun):
        output.append(
            {
                "simulation_id": i,
                "-c": data["-c"],
                "-x": data["-x"],
                "-s": grompp["-o"],
            }
        )
    return output


with DAG(
    dag_id="simulate_expand_uniform",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "inputs": Param(
            {
                "mdp": {"directory": "mdp", "filename": "npt.json"},
                "gro": {
                    "directory": "nvt_equil",
                    "filename": "nvt.gro",
                    "ref_data": False,
                },
                "top": {
                    "directory": "prep",
                    "filename": "system_prepared.top",
                    "ref_data": False,
                },
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
        "mdp_options": {"ref_t": 300},
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
    mdp = update_write_mdp_json_as_mdp_from_file.override(task_id="write_mdp")(
        mdp_json_file_path=mdp_json,
        update_dict="{{ params.mdp_options }}",
    )
    top = get_file.override(task_id="get_top")(
        input_dir="{{ params.inputs.top.directory }}",
        file_name="{{ params.inputs.top.filename }}",
        use_ref_data="{{ params.inputs.top.ref_data }}",
    )
    gro = get_file.override(task_id="get_gro")(
        file_name="{{ params.inputs.gro.filename }}",
        input_dir="{{ params.inputs.gro.directory }}",
        use_ref_data="{{ params.inputs.gro.ref_data }}",
    )

    grompp = ResourceGmxOperator(
        task_id="grompp",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_arguments=["grompp"],
        input_files={"-f": mdp, "-c": gro, "-p": top},
        output_files={"-o": "{{ params.expected_output | replace('.gro', '.tpr') }}"},
        output_dir="{{ params.output_dir }}",
    )
    outputs_dirs = outputs_list.override(task_id="get_output_dirs")()
    mdrun = ResourceGmxOperator.partial(
        task_id="mdrun",
        executor_config={
            "mpi_ranks": 1,
            "cpus_per_task": 1,
            "gpus": 0,
            "gpu_type": None,
        },
        gmx_executable="gmx_mpi",
        gmx_arguments=["mdrun"],
        input_files={"-s": "{{ ti.xcom_pull(task_ids='grompp')['-o'] }}"},
        output_files={
            "-c": "{{ params.expected_output }}",
            "-x": "{{ params.expected_output | replace('.gro', '.xtc') }}",
        },
    ).expand(output_dir=outputs_dirs)
    grompp >> mdrun

    sims_data = prepare_output(mdrun.output, grompp.output)
    dataset_dict = dataset_from_xcom_dicts.override(task_id="make_dataset")(
        output_dir="{{ params.output_dir }}",
        output_fn="{{ params.expected_output | replace('.gro', '.json') }}",
        list_of_dicts=sims_data,
        dataset_structure="{{ params.output_dataset_structure }}",
    )
    sims_data >> dataset_dict
