import os
import pytest

from unittest.mock import patch, MagicMock
from dataclasses import dataclass
from tests.conftest import DEFAULT_DATE, session, dag_maker

data_path = os.path.join(os.path.dirname(__file__), "data")


@dataclass
class RefData:
    shift_range: int = 1
    num_iterations: int = 3
    num_simulations: int = 4
    num_states: int = 9
    num_steps: int = 2000
    temperature: float = 298


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_store_dhdl_results(dag_maker, session):
    from airflowHPC.dags.replex import store_dhdl_results
    import tempfile
    import json

    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]
    temp_out_dir = tempfile.mkdtemp()
    output_fn = "dhdl_store.json"
    iteration_idx = 1
    dhdl_dict = {str(iteration_idx): dhdl_files}

    with dag_maker(
        "test_store_dhdl_results-dag", session=session, start_date=DEFAULT_DATE
    ) as dag:
        dhdl_store = store_dhdl_results(
            dhdl_dict=dhdl_dict,
            output_dir=temp_out_dir,
            output_fn=output_fn,
            iteration=iteration_idx,
        )

    dr = dag_maker.create_dagrun()
    dhdl_store.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    ti_dhdl_store = dr.get_task_instances()[0]

    with open(ti_dhdl_store.xcom_pull().uri, "r") as f:
        data = json.load(f)

    assert data["iteration"][str(iteration_idx)] == dhdl_files


def test_initialize_MDP(dag_maker, session, ref_data=RefData()):
    import tempfile
    from airflowHPC.dags.replex import initialize_MDP
    from airflowHPC.data import data_dir as data
    from airflowHPC.utils.mdp2json import mdp2json

    input_mdp = os.path.abspath(os.path.join(data, "ensemble_md", "expanded.mdp"))
    temp_out_dir = tempfile.mkdtemp()
    expand_args = [
        {"simulation_id": i, "output_dir": f"{temp_out_dir}/sim_{i}/iteration_1"}
        for i in range(4)
    ]
    with dag_maker(
        "test_initialize_MDP-dag", session=session, start_date=DEFAULT_DATE
    ) as dag:
        output_mdp = (
            initialize_MDP.override(task_id="output_mdp")
            .partial(
                template_mdp=input_mdp,
                num_simulations=ref_data.num_simulations,
                num_steps=ref_data.num_steps,
                num_states=ref_data.num_states,
                shift_range=ref_data.shift_range,
            )
            .expand(expand_args=expand_args)
        )

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    for ti in tis:
        ti.run()

    expected = [
        {
            "vdw_lambdas": [0.0, 0.0, 0.0, 0.0, 0.0, 0.25],
            "coul_lambdas": [0.0, 0.25, 0.5, 0.75, 1.0, 1.0],
            "init_lambda_weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
        {
            "vdw_lambdas": [0.0, 0.0, 0.0, 0.0, 0.25, 0.5],
            "coul_lambdas": [0.25, 0.5, 0.75, 1.0, 1.0, 1.0],
            "init_lambda_weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
        {
            "vdw_lambdas": [0.0, 0.0, 0.0, 0.25, 0.5, 0.75],
            "coul_lambdas": [0.5, 0.75, 1.0, 1.0, 1.0, 1.0],
            "init_lambda_weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
        {
            "vdw_lambdas": [0.0, 0.0, 0.25, 0.5, 0.75, 1.0],
            "coul_lambdas": [0.75, 1.0, 1.0, 1.0, 1.0, 1.0],
            "init_lambda_weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        },
    ]

    output_mdp_paths = [ti.xcom_pull()[i] for i, ti in enumerate(tis)]

    for i, mdp_path in enumerate(output_mdp_paths):
        assert os.path.exists(mdp_path)
        mdp_data = mdp2json(mdp_path)
        assert mdp_data["nsteps"] == ref_data.num_steps
        assert [float(j) for j in mdp_data["vdw_lambdas"].split(" ")] == expected[i][
            "vdw_lambdas"
        ]
        assert [float(j) for j in mdp_data["coul_lambdas"].split(" ")] == expected[i][
            "coul_lambdas"
        ]
        assert [
            float(j) for j in mdp_data["init_lambda_weights"].split(" ")
        ] == expected[i]["init_lambda_weights"]


def test_update_MDP(dag_maker, session, ref_data=RefData()):
    import tempfile, shutil
    from airflowHPC.dags.replex import update_MDP
    from airflowHPC.data import data_dir as data
    from airflowHPC.utils.mdp2json import mdp2json

    input_mdp = os.path.abspath(os.path.join(data, "ensemble_md", "expanded.mdp"))
    iteration_idx = 3
    temp_out_dir = tempfile.mkdtemp()
    expand_args = [
        {"simulation_id": i, "output_dir": f"{temp_out_dir}/sim_{i}/iteration_1"}
        for i in range(4)
    ]
    for arg in expand_args:
        os.makedirs(arg["output_dir"], exist_ok=True)
        shutil.copy(input_mdp, arg["output_dir"])
        arg["template"] = os.path.join(arg["output_dir"], "expanded.mdp")

    mock_dhdl_store = MagicMock()
    mock_dhdl_store.uri = os.path.join(data_path, "mock_dhdl_store.json")
    with patch(
        "airflowHPC.dags.replex.store_dhdl_results", return_value=mock_dhdl_store
    ):
        with dag_maker(
            "test_update_mdp-dag", session=session, start_date=DEFAULT_DATE
        ) as dag:
            output_mdp = (
                update_MDP.override(task_id="output_mdp")
                .partial(
                    iter_idx=iteration_idx,
                    dhdl_store=mock_dhdl_store,
                    num_simulations=ref_data.num_simulations,
                    num_steps=ref_data.num_steps,
                    shift_range=ref_data.shift_range,
                )
                .expand(expand_args=expand_args)
            )

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    for ti in tis:
        ti.run()

    expected = [
        {"init_lambda_state": 2},
        {"init_lambda_state": 4},
        {"init_lambda_state": 5},
        {"init_lambda_state": 1},
    ]

    output_mdp_paths = [ti.xcom_pull()[i] for i, ti in enumerate(tis)]
    for i, mdp_path in enumerate(output_mdp_paths):
        assert os.path.exists(mdp_path)
        mdp_data = mdp2json(mdp_path)
        assert mdp_data["nsteps"] == ref_data.num_steps
        assert mdp_data["tinit"] == 12
        assert mdp_data["init_lambda_state"] == expected[i]["init_lambda_state"]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize(
    "proposal, result_states, expected_result",
    [
        ("exhaustive", {0: 0, 1: 6, 2: 7, 3: 8}, [0, 1, 2, 3]),  # no swap
        ("single", {0: 5, 1: 2, 2: 2, 3: 8}, [0, 2, 1, 3]),  # swap (1, 2)
        ("neighboring", {0: 5, 1: 2, 2: 2, 3: 8}, [0, 2, 1, 3]),  # swap (1, 2)
        ("exhaustive", {0: 5, 1: 2, 2: 2, 3: 8}, [0, 2, 1, 3]),  # swap (1, 2)
        ("exhaustive", {0: 4, 1: 2, 2: 4, 3: 3}, [1, 0, 3, 2]),  # swap (2, 3), (0, 1)
    ],
)
def test_get_swaps(
    dag_maker, session, proposal, result_states, expected_result, ref_data=RefData()
):
    import tempfile
    import random
    from airflowHPC.dags.replex import (
        get_swaps,
        store_dhdl_results,
        extract_final_dhdl_info,
        reduce_dhdl,
    )

    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]
    iteration_idx = 0
    results = [
        {"simulation_id": 0, "gro_path": "result_0.gro", "dhdl": dhdl_files[0]},
        {"simulation_id": 1, "gro_path": "result_1.gro", "dhdl": dhdl_files[1]},
        {"simulation_id": 2, "gro_path": "result_2.gro", "dhdl": dhdl_files[2]},
        {"simulation_id": 3, "gro_path": "result_3.gro", "dhdl": dhdl_files[3]},
    ]

    with dag_maker(
        "test_get_swaps-dag", session=session, start_date=DEFAULT_DATE
    ) as dag:
        dhdl_results = extract_final_dhdl_info.partial(shift_range=1).expand(
            result=results
        )
        reduce_dhdl(dhdl_results, iteration_idx)

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    for ti in tis:
        ti.run()

    ti_reduce_dhdl = dr.get_task_instance(task_id="reduce_dhdl")
    dhdl_dict = ti_reduce_dhdl.xcom_pull()
    for info in dhdl_dict[str(iteration_idx)]:
        info["state"] = result_states[info["simulation_id"]]

    temp_out_dir = tempfile.mkdtemp()
    output_fn = "dhdl_store.json"
    dhdl_store = store_dhdl_results.override(dag=dag)(
        dhdl_dict=dhdl_dict,
        output_dir=temp_out_dir,
        output_fn=output_fn,
        iteration=iteration_idx,
    )
    dhdl_store.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    random.seed(0)  # to ensure reproducibility
    swap_pattern = get_swaps.override(dag=dag)(
        iteration=iteration_idx,
        dhdl_store=dhdl_store,
        num_simulations=ref_data.num_simulations,
        num_states=ref_data.num_states,
        shift_range=ref_data.shift_range,
        temperature=ref_data.temperature,
        proposal=proposal,
    )
    swap_pattern.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    ti_get_swaps = dr.get_task_instance(task_id="get_swaps")

    assert ti_get_swaps.xcom_pull() == expected_result


def test_extract_final_dhdl_info(dag_maker, session, ref_data=RefData()):
    from airflowHPC.dags.replex import extract_final_dhdl_info

    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]
    results = [
        {"simulation_id": 0, "gro_path": "result_0.gro", "dhdl": dhdl_files[0]},
        {"simulation_id": 1, "gro_path": "result_1.gro", "dhdl": dhdl_files[1]},
        {"simulation_id": 2, "gro_path": "result_2.gro", "dhdl": dhdl_files[2]},
        {"simulation_id": 3, "gro_path": "result_3.gro", "dhdl": dhdl_files[3]},
    ]
    result_states = {0: 5, 1: 2, 2: 2, 3: 8}

    with dag_maker(
        "test_extract_final_dhdl_info-dag", session=session, start_date=DEFAULT_DATE
    ) as dag:
        dhdl_result = extract_final_dhdl_info.partial(
            shift_range=ref_data.shift_range
        ).expand(result=results)

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    for ti in tis:
        ti.run()

    outputs = [ti.xcom_pull()[i] for i, ti in enumerate(tis)]
    for data in outputs:
        assert data["state"] == result_states[data["simulation_id"]]


def test_accept_or_reject():
    from airflowHPC.dags.replex import accept_or_reject
    import random

    random.seed(0)
    swap_bool_1 = accept_or_reject(0)
    swap_bool_2 = accept_or_reject(0.8)  # rand = 0.844
    swap_bool_3 = accept_or_reject(0.8)  # rand = 0.758

    assert swap_bool_1 is False
    assert swap_bool_2 is False
    assert swap_bool_3 is True


def test_propose_swap():
    from airflowHPC.dags.replex import propose_swap
    import random

    random.seed(0)
    swap_1 = propose_swap([])
    swap_2 = propose_swap([(0, 1), (0, 2), (1, 2)])
    assert swap_1 == []
    assert swap_2 == (1, 2)


@pytest.mark.parametrize(
    "swap, prob_acc, info",
    [
        (
            (0, 1),
            0.45968522728859024,
            "U^i_n - U^i_m = -3.69 kT, U^j_m - U^j_n = 4.46 kT, Total dU: 0.78 kT",
            # dU = (-9.1366697  + 11.0623788)/2.4777098766670016 ~ 0.7772 kT, so p_acc = 0.45968522728859024
        ),
        (
            (0, 2),
            1,
            "U^i_n - U^i_m = -3.69 kT, U^j_m - U^j_n = 2.02 kT, Total dU: -1.67 kT",
            # dU = (-9.1366697 + 4.9963939)/2.4777098766670016 ~ -1.6710 kT, so p_acc = 1
        ),
    ],
)
def test_calc_prob_acc(capfd, swap, prob_acc, info, ref_data=RefData()):
    from airflowHPC.dags.replex import calc_prob_acc

    # k = 1.380649e-23; NA = 6.0221408e23; T = 298; kT = k * NA * T / 1000 = 2.4777098766670016
    # state_ranges = [[0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6], ..., [3, 4, 5, 6, 7, 8]]
    states = [5, 2, 2, 8]
    shifts = [0, 1, 2, 3]
    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]

    prob_acc_1 = calc_prob_acc(
        swap=swap,
        dhdl_files=dhdl_files,
        states=states,
        shifts=shifts,
        num_states=ref_data.num_states,
        shift_range=ref_data.shift_range,
        num_simulations=ref_data.num_simulations,
        temperature=ref_data.temperature,
    )
    out, err = capfd.readouterr()
    assert prob_acc_1 == pytest.approx(prob_acc)
    assert info in out
