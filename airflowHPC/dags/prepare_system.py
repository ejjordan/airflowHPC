import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.models.param import Param
from airflowHPC.dags.tasks import get_file, run_gmxapi, branch_task_template
from airflowHPC.utils.mdp2json import write_mdp_json_as_mdp


with DAG(
    "prepare_system",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "inputs": Param(
            {"pdb": {"directory": "fs_peptide", "filename": "fs.pdb"}},
            type=["object", "null"],
            title="Inputs",
            items={
                "type": "object",
                "properties": {"pdb": {"type": ["object", "null"]}},
                "required": ["pdb"],
            },
            section="Data",
        ),
        "outputs": Param(
            {
                "directory": "prep",
                "gro": "system_prepared.gro",
                "top": "system_prepared.top",
            },
            type=["object", "null"],
            title="Outputs",
            items={
                "type": "object",
                "properties": {
                    "directory": {"type": "string"},
                    "gro": {"type": "string"},
                    "top": {"type": "string"},
                },
                "required": ["directory", "gro", "top"],
            },
            section="Data",
        ),
        "box_size": 4,
        "force_field": "amber99sb-ildn",
        "water_model": "tip3p",
        "ion_concentration": Param(
            0.15,
            type="number",
            title="Ion concentration",
        ),
    },
) as prepare:
    prepare.doc = """Generic gromacs setup. If param ion_concentration is 0.0, genion is not run."""

    input_pdb = get_file.override(task_id="get_pdb")(
        input_dir="{{ params.inputs.pdb.directory }}",
        file_name="{{ params.inputs.pdb.filename }}",
    )
    pdb2gmx = run_gmxapi.override(task_id="pdb2gmx")(
        args=[
            "pdb2gmx",
            "-ff",
            "{{ params.force_field }}",
            "-water",
            "{{ params.water_model }}",
        ],
        input_files={"-f": input_pdb},
        output_files={"-o": "system_initial.gro", "-p": "topol.top", "-i": "posre.itp"},
        output_dir="{{ params.outputs.directory }}",
    )
    rename_pdb2gmx_top = BashOperator(
        task_id="rename_pdb2gmx_top",
        bash_command="cp {{ task_instance.xcom_pull(task_ids='pdb2gmx', key='-p') }} {{ params.outputs.directory }}/pdb2gmx.top",
        cwd=os.path.curdir,
    )

    editconf = run_gmxapi.override(task_id="editconf")(
        args=[
            "editconf",
            "-c",
            "-box",
            "{{ params.box_size }}",
            "{{ params.box_size }}",
            "{{ params.box_size }}",
        ],
        input_files={"-f": pdb2gmx["-o"]},
        output_files={"-o": "system_box.gro"},
        output_dir="{{ params.outputs.directory }}",
    )
    # gmx solvate does not allow specifying different file names for input and output top files.
    # Thus, we have to manually manage the files for each stage in the pipeline.
    solvate = run_gmxapi.override(task_id="solvate")(
        args=["solvate"],
        input_files={"-cp": editconf["-o"], "-cs": "spc216.gro", "-p": pdb2gmx["-p"]},
        output_files={"-o": "system_solv.gro"},
        output_dir="{{ params.outputs.directory }}",
    )
    rename_solvate_top = BashOperator(
        task_id="rename_solvate_top",
        bash_command="cp {{ task_instance.xcom_pull(task_ids='pdb2gmx', key='-p') }} {{ params.outputs.directory }}/solvate.top",
        cwd=os.path.curdir,
    )

    rename_solvate_gro_output = BashOperator(
        task_id="rename_solvate_gro_output",
        bash_command="mv {{ params.outputs.directory }}/system_solv.gro {{ params.outputs.directory }}/{{ params.outputs.gro }}",
        cwd=os.path.curdir,
    )
    rename_solvate_top_output = BashOperator(
        task_id="rename_solvate_top_output",
        bash_command="mv {{ task_instance.xcom_pull(task_ids='pdb2gmx', key='-p') }} {{ params.outputs.directory }}/{{ params.outputs.top }}",
        cwd=os.path.curdir,
    )

    prepare_done_branch = branch_task_template.override(task_id="prepare_done_branch")(
        statement="{{ params.ion_concentration }} > 0.0",
        task_if_true=rename_solvate_top.task_id,
        task_if_false=rename_solvate_gro_output.task_id,
    )
    genion_mdp = write_mdp_json_as_mdp.override(task_id="genion_mdp")(mdp_data={})
    genion_grompp = run_gmxapi.override(task_id="genion_grompp")(
        args=["grompp"],
        input_files={"-f": genion_mdp, "-c": solvate["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "ions.tpr"},
        output_dir="{{ params.outputs.directory }}",
    )
    genion = run_gmxapi.override(task_id="genion")(
        args=[
            "genion",
            "-neutral",
            "-conc",
            "{{ params.ion_concentration }}",
            "-pname",
            "NA",
            "-nname",
            "CL",
        ],
        input_files={"-s": genion_grompp["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "system_solv_ions.gro"},
        output_dir="{{ params.outputs.directory }}",
        stdin="SOL",
    )
    rename_genion_gro_output = BashOperator(
        task_id="rename_genion_gro_output",
        bash_command="mv {{ params.outputs.directory }}/system_solv_ions.gro {{ params.outputs.directory }}/{{ params.outputs.gro }}",
        cwd=os.path.curdir,
    )
    rename_genion_top_output = BashOperator(
        task_id="rename_genion_top_output",
        bash_command="mv {{ task_instance.xcom_pull(task_ids='pdb2gmx', key='-p') }} {{ params.outputs.directory }}/{{ params.outputs.top }}",
        cwd=os.path.curdir,
    )

    input_pdb >> pdb2gmx >> rename_pdb2gmx_top >> solvate
    solvate >> prepare_done_branch >> [rename_solvate_top, rename_solvate_gro_output]
    prepare_done_branch >> [rename_solvate_gro_output, rename_solvate_top_output]
    (
        rename_solvate_top
        >> genion_mdp
        >> genion_grompp
        >> genion
        >> [rename_genion_gro_output, rename_genion_top_output]
    )
