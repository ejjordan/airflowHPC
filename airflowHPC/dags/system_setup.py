from airflow import DAG
from airflow.utils import timezone
from airflowHPC.dags.tasks import get_file, run_gmxapi
from airflowHPC.utils.mdp2json import update_write_mdp_json_as_mdp_from_file



with DAG(
    "system_setup",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"input_dir": "ala_tripeptide_remd", "output_dir": "prep", "input_pdb": "ala_tripeptide.pdb", "box_size": 3},
) as system_setup:
    system_setup.doc = """Generic gromacs setup."""

    input_pdb = get_file.override(task_id="get_pdb")(
        input_dir="{{ params.input_dir }}", file_name="{{ params.input_pdb }}"
    )
    pdb2gmx = run_gmxapi.override(task_id="pdb2gmx")(
        args=["pdb2gmx", "-ff", "charmm27", "-water", "tip3p"],
        input_files={"-f": input_pdb},
        output_files={"-o": "system.gro", "-p": "topol.top", "-i": "posre.itp"},
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
    mdp_json_em = get_file.override(task_id="get_min_mdp_json")(
        input_dir="{{ params.input_dir }}", file_name="min.json"
    )
    mdp_em = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_em_update")(mdp_json_file_path=mdp_json_em)
    grompp_em = run_gmxapi.override(task_id="grompp_em")(
        args=["grompp"],
        input_files={"-f": mdp_em, "-c": solvate["-o"], "-p": pdb2gmx["-p"]},
        output_files={"-o": "em.tpr"},
        output_dir="{{ params.output_dir }}",
    )
    mdrun_em = run_gmxapi.override(task_id="mdrun_em")(
        args=["mdrun", "-v", "-deffnm", "em"],
        input_files={"-s": grompp_em["-o"]},
        output_files={"-c": "em.gro"},
        output_dir="{{ params.output_dir }}",
    )
    mdp_json_nvt = get_file.override(task_id="get_nvt_mdp_json")(
        input_dir="{{ params.input_dir }}", file_name="nvt.json"
    )
    mdp_nvt = update_write_mdp_json_as_mdp_from_file.override(task_id="mdp_nvt_update")(mdp_json_file_path=mdp_json_nvt)
    grompp_nvt = run_gmxapi.override(task_id="grompp_nvt")(
        args=["grompp"],
        input_files={
            "-f": mdp_nvt,
            "-c": mdrun_em["-c"],
            "-r": mdrun_em["-c"],
            "-p": pdb2gmx["-p"],
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
