from airflow.decorators import task
from dataclasses import dataclass

__all__ = ("run_grompp", "run_mdrun", "InputHolder")


@dataclass
class InputHolder:
    gro_path: str
    top_path: str
    mdp_path: str
    simulation_id: int
    output_dir: str


@dataclass
class MdrunHolder:
    tpr_path: str
    output_dir: str
    simulation_id: int


@task
def prepare_input(counter, num_simulations):
    import os
    from dataclasses import asdict
    from airflowHPC.data import data_dir

    input_dir = os.path.abspath(os.path.join(data_dir, "ensemble_md"))
    inputHolderList = [
        asdict(
            InputHolder(
                gro_path=os.path.join(input_dir, "sys.gro"),
                top_path=os.path.join(input_dir, "sys.top"),
                mdp_path=os.path.join(input_dir, "expanded.mdp"),
                simulation_id=i,
                output_dir=f"outputs/step_{counter}/sim_{i}",
            )
        )
        for i in range(num_simulations)
    ]
    return inputHolderList


@task(multiple_outputs=True, queue="radical")
def run_grompp(input_holder_dict, verbose: bool = False):
    import os
    import gmxapi as gmx
    from dataclasses import asdict

    input_holder = InputHolder(**input_holder_dict)
    input_files = {
        "-f": input_holder.mdp_path,
        "-p": input_holder.top_path,
        "-c": input_holder.gro_path,
    }
    output_dir = input_holder.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    tpr = os.path.join(out_path, "run.tpr")
    output_files = {"-o": tpr}
    cwd = os.getcwd()
    os.chdir(out_path)
    grompp = gmx.commandline_operation(
        gmx.commandline.cli_executable(), "grompp", input_files, output_files
    )
    grompp.run()
    os.chdir(cwd)
    if verbose:
        print(grompp.output.stderr.result())
    assert os.path.exists(grompp.output.file["-o"].result())
    return asdict(
        MdrunHolder(
            tpr_path=grompp.output.file["-o"].result(),
            output_dir=out_path,
            simulation_id=input_holder.simulation_id,
        )
    )


@task(multiple_outputs=True, max_active_tis_per_dag=1, queue="radical")
def run_mdrun(mdrun_holder_dict) -> dict:
    import os
    import gmxapi as gmx

    mdrun_holder = MdrunHolder(**mdrun_holder_dict)
    output_dir = mdrun_holder.output_dir
    simulation_id = mdrun_holder.simulation_id
    tpr_path = mdrun_holder.tpr_path
    if not os.path.exists(tpr_path):
        raise FileNotFoundError("You must supply a tpr file")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    input_files = {"-s": tpr_path}
    output_files = {
        "-x": os.path.join(out_path, "result.xtc"),
        "-c": os.path.join(out_path, "result.gro"),
        "-dhdl": os.path.join(out_path, "dhdl.xvg"),
    }
    cwd = os.getcwd()
    os.chdir(out_path)
    md = gmx.commandline_operation(
        gmx.commandline.cli_executable(), "mdrun", input_files, output_files
    )
    md.run()
    os.chdir(cwd)
    assert os.path.exists(md.output.file["-c"].result())
    results_dict = md.output.file.result()
    results_dict["simulation_id"] = simulation_id
    return results_dict
