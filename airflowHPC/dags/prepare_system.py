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
            title="Inputs list",
            items={
                "type": "object",
                "properties": {"pdb": {"type": ["object", "null"]}},
                "required": ["pdb"],
            },
            section="inputs",
        ),
        "output_dir": "prep",
        "box_size": 4,
        "force_field": "amber99sb-ildn",
        "water_model": "tip3p",
        "ion_concentration": Param(
            0.15,
            type="number",
            title="Ion concentration",
        )
    },
) as prepare:
    prepare.doc = """Generic gromacs setup."""

    input_pdb = get_file.override(task_id="get_pdb")(
        input_dir="{{ params.inputs.pdb.directory }}", file_name="{{ params.inputs.pdb.filename }}"
    )
    pdb2gmx = run_gmxapi.override(task_id="pdb2gmx")(
        args=["pdb2gmx", "-ff", "{{ params.force_field }}", "-water", "{{ params.water_model }}"],
        input_files={"-f": input_pdb},
        output_files={"-o": "system_initial.gro", "-p": "topol.top", "-i": "posre.itp"},
        output_dir="{{ params.output_dir }}",
    )
    editconf = run_gmxapi.override(task_id="editconf")(
        args=["editconf", "-c", "-box", "{{ params.box_size }}", "{{ params.box_size }}", "{{ params.box_size }}"],
        input_files={"-f": pdb2gmx["-o"]},
        output_files={"-o": "system_box.gro"},
        output_dir="{{ params.output_dir }}",
    )
    # gmx solvate does not allow specifying different file names for input and output top files.
    # Here we rely on the fact that solvate overwrites the input top file with the solvated top file.
    # Thus, after solvate, pdb2gmx["-p"] is what should be solvate["-p"].
    solvate = run_gmxapi.override(task_id="solvate")(
        args=["solvate"],
        input_files={"-cp": editconf["-o"], "-cs": "spc216.gro", "-p": pdb2gmx["-p"]},
        output_files={"-o": "system_solv.gro"},
        output_dir="{{ params.output_dir }}",
    )
    rename_after_solvate = BashOperator(
        task_id="rename_solvate",
        bash_command="cp {{ params.output_dir }}/system_solv.gro {{ params.output_dir }}/system_prepared.gro",
        cwd=os.path.curdir,
    )
    genion_mdp = write_mdp_json_as_mdp.override(task_id="genion_mdp")(mdp_data={})
    prepare_done_branch = branch_task_template.override(task_id="prepare_done_branch")(
        statement="{{ params.ion_concentration }} > 0.0",
        task_if_true="genion_mdp",
        task_if_false=rename_after_solvate.task_id,
    )
    genion_grompp = run_gmxapi.override(task_id="genion_grompp")(
        args=["grompp"],
        input_files={"-f": genion_mdp, "-c": solvate["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "ions.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    genion = run_gmxapi.override(task_id="genion")(
        args=["genion", "-neutral", "-conc", "{{ params.ion_concentration }}", "-pname", "NA", "-nname", "CL"],
        input_files={"-s": genion_grompp["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "system_solv_ions.gro"},
        output_dir="{{ params.output_dir }}",
        stdin="SOL",
    )
    rename_after_genion = BashOperator(
        task_id="rename_genion",
        bash_command="cp {{ params.output_dir }}/system_solv_ions.gro {{ params.output_dir }}/system_prepared.gro",
        cwd=os.path.curdir,
    )

    solvate >> prepare_done_branch >> [genion_mdp, rename_after_solvate]
    genion_mdp >> genion_grompp >> genion >> rename_after_genion

