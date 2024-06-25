import os
import pytest

from airflow.operators.python import PythonOperator

from tests.conftest import BasePythonTest, DEFAULT_DATE
from airflowHPC.dags.replex import calc_prob_acc, store_dhdl_results

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


def test_calc_prob_acc(capfd):
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
