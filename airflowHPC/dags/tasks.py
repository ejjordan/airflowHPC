from airflow.decorators import task
from airflowHPC.data import data_dir

__all__ = ("run_grompp", "run_mdrun")


@task(multiple_outputs=True)
def run_grompp(input_gro: str, output_dir: str, verbose: bool = False):
    import os
    import gmxapi as gmx

    input_dir = os.path.abspath(os.path.join(data_dir, "ensemble_md"))
    input_top = os.path.join(input_dir, "sys.top")
    input_mdp = os.path.join(input_dir, "expanded.mdp")
    input_gro = os.path.join(input_dir, input_gro)
    input_files = {
        "-f": input_mdp,
        "-p": input_top,
        "-c": input_gro,
    }
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
    return grompp.output.file.result()


@task(multiple_outputs=True)
def run_mdrun(tpr_path: str, output_dir: str, verbose: bool = False):
    import os
    import gmxapi as gmx

    if not os.path.exists(tpr_path):
        raise FileNotFoundError("You must supply a tpr file")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    os.chdir(output_dir)
    cwd = os.getcwd()
    input_files = {"-s": tpr_path}
    output_files = {
        "-x": os.path.join(cwd, "result.xtc"),
        "-c": os.path.join(cwd, "result.gro"),
    }
    md = gmx.commandline_operation(
        gmx.commandline.cli_executable(), "mdrun", input_files, output_files
    )
    md.run()
    if verbose:
        print(md.output.stderr.result())
    assert os.path.exists(md.output.file["-c"].result())
    return md.output.file.result()
