import os
import pytest

from airflow.operators.python import PythonOperator

from tests.conftest import BasePythonTest, DEFAULT_DATE, session

data_path = os.path.join(os.path.dirname(__file__), "data")


class TestReplex(BasePythonTest):
    default_date = DEFAULT_DATE
    opcls = PythonOperator

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_store_dhdl_results(self):
        from airflowHPC.dags.replex import store_dhdl_results
        import tempfile
        import json

        data_path = os.path.join(os.path.dirname(__file__), "data")
        dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]
        temp_out_dir = tempfile.mkdtemp()
        iteration_idx = 1
        dhdl_dict = {str(iteration_idx): dhdl_files}

        with self.dag:
            dhdl_store = store_dhdl_results(
                dhdl_dict=dhdl_dict, output_dir=temp_out_dir, iteration=iteration_idx
            )

        dr = self.create_dag_run()
        dhdl_store.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ti_dhdl_store = dr.get_task_instances()[0]

        with open(ti_dhdl_store.xcom_pull().uri, "r") as f:
            data = json.load(f)

        assert data["iteration"][str(iteration_idx)] == dhdl_files


def test_extract_final_dhdl_info(dag_maker, session):
    from airflowHPC.dags.replex import extract_final_dhdl_info

    data_path = os.path.join(os.path.dirname(__file__), "data")
    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]
    results = [
        {"simulation_id": 0, "gro_path": "result_0.gro", "dhdl": dhdl_files[0]},
        {"simulation_id": 1, "gro_path": "result_1.gro", "dhdl": dhdl_files[1]},
        {"simulation_id": 2, "gro_path": "result_2.gro", "dhdl": dhdl_files[2]},
        {"simulation_id": 3, "gro_path": "result_3.gro", "dhdl": dhdl_files[3]},
    ]
    result_states = {0: 5, 1: 2, 2: 2, 3: 8}

    with dag_maker("test-dag", session=session, start_date=DEFAULT_DATE) as dag:
        dhdl_result = extract_final_dhdl_info.expand(result=results)

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


def test_calc_prob_acc(capfd):
    from airflowHPC.dags.replex import calc_prob_acc

    # k = 1.380649e-23; NA = 6.0221408e23; T = 298; kT = k * NA * T / 1000 = 2.4777098766670016
    # state_ranges = [[0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6], ..., [3, 4, 5, 6, 7, 8]]
    states = [5, 2, 2, 8]
    shifts = [0, 1, 2, 3]
    dhdl_files = [os.path.join(data_path, f"dhdl/dhdl_{i}.xvg") for i in range(4)]

    # Test 1
    swap = (0, 1)
    prob_acc_1 = calc_prob_acc(swap, dhdl_files, states, shifts)
    out, err = capfd.readouterr()
    # dU = (-9.1366697  + 11.0623788)/2.4777098766670016 ~ 0.7772 kT, so p_acc = 0.45968522728859024
    assert prob_acc_1 == pytest.approx(0.45968522728859024)
    assert "U^i_n - U^i_m = -3.69 kT, U^j_m - U^j_n = 4.46 kT, Total dU: 0.78 kT" in out

    # Test 2
    swap = (0, 2)
    prob_acc_2 = calc_prob_acc(swap, dhdl_files, states, shifts)
    # dU = (-9.1366697 + 4.9963939)/2.4777098766670016 ~ -1.6710 kT, so p_acc = 1
    assert prob_acc_2 == 1
