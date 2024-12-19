from airflowHPC.hooks.slurm import SlurmHook
import unittest.mock


def test_mock_sacct():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        assert slurm_hook.node_names == ["nid000123", "nid000456"]
        assert slurm_hook.num_nodes == 2
        assert slurm_hook.tasks_per_node == 64
        assert slurm_hook.cpus_per_task == 2
