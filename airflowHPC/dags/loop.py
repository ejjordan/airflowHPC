from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow import Dataset

from tasks import run_grompp


@task.branch
def check_condition(counter, **kwargs):
    ti = kwargs["ti"]
    time = ti.xcom_pull(task_ids="time_holder", key="time")
    print(f"\nTime is {time}\n")
    if counter == 0:
        return "run_mdrun"
    elif time < 4:
        return "run_mdrun"
    else:
        return "run_complete"


@task
def run_complete():
    print("run complete")


@task(multiple_outputs=True, max_active_tis_per_dag=1)
def run_mdrun(tpr_path: str, output_info) -> dict:
    import os
    import gmxapi as gmx

    output_dir = output_info[0]
    simulation_id = output_info[1]
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


def get_xtc(result):
    return result["-x"]


def get_dhdl(result):
    return {"simulation_id": result["simulation_id"], "-dhdl": result["-dhdl"]}


def get_state(result):
    return {"simulation_id": result["simulation_id"], "state": result["state"]}


@task
def extract_final_dhdl_info(result) -> dict[str, int]:
    from alchemlyb.parsing.gmx import _get_headers as get_headers
    from alchemlyb.parsing.gmx import _extract_dataframe as extract_dataframe

    shift_range = 1
    i = result["simulation_id"]
    dhdl = result["-dhdl"]
    headers = get_headers(dhdl)
    state_local = list(extract_dataframe(dhdl, headers=headers)["Thermodynamic state"])[
        -1
    ]  # local index of the last state  # noqa: E501
    state_global = state_local + i * shift_range  # global index of the last state
    return {"simulation_id": i, "state": state_global}


@task
def store_dhdl_results(dhdl, output_dir, iteration) -> Dataset:
    import os
    import json

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.abspath(output_dir)
    output_file = os.path.join(out_path, "dhdl.json")
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            data = json.load(f)
        if str(iteration) in data["iteration"].keys():
            data["iteration"][str(iteration)].append(dhdl)
        else:
            data["iteration"][str(iteration)] = [dhdl]
        with open(output_file, "w") as f:
            json.dump(data, f, indent=4, separators=(",", ": "))
    else:
        with open(output_file, "w") as f:
            data = {"iteration": {str(iteration): [dhdl]}}
            json.dump(data, f, indent=4, separators=(",", ": "))
    return Dataset(f"file:///{output_file}")


with DAG(
    "looper",
    schedule=None,
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
) as dag:
    grompp_result = run_grompp("sys.gro", "outputs/grompp")
    mdrun_outputs_info = [(f"outputs/sim_{i}", i) for i in range(2)]
    mdrun_result = run_mdrun.partial(tpr_path=grompp_result["-o"]).expand(
        output_info=mdrun_outputs_info
    )
    dhdl = mdrun_result.map(get_dhdl)
    dhdl_result = extract_final_dhdl_info.expand(result=dhdl)
    state = dhdl_result.map(get_state)
    dhdl_store = store_dhdl_results.partial(
        output_dir="outputs/dhdl", iteration=0
    ).expand(dhdl=state)
