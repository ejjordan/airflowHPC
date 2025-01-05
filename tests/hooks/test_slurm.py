import pytest
from contextlib import nullcontext
from airflowHPC.hooks.slurm import SlurmHook
import unittest.mock
from airflow.models.taskinstancekey import TaskInstanceKey


def test_init():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        assert slurm_hook.node_names == ["nid000123", "nid000456"]
        assert slurm_hook.num_nodes == 2
        assert slurm_hook.tasks_per_node == 16
        assert slurm_hook.cpus_per_task == 2


def test_set_task_resources():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        task_instance_key = TaskInstanceKey("dag_id", "task_id", "run_id")
        num_ranks = 4
        num_threads = 2
        num_gpus = 0
        slurm_hook.set_task_resources(
            task_instance_key, num_ranks, num_threads, num_gpus
        )
        task_resource_req = slurm_hook.task_resource_requests[task_instance_key]
        assert task_resource_req.num_ranks == num_ranks
        assert task_resource_req.num_threads == num_threads
        assert task_resource_req.num_gpus == num_gpus


def test_find_available_slots():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        ti_key1 = TaskInstanceKey("dag_id", "task1", "run_id")
        ti_key2 = TaskInstanceKey("dag_id", "task2", "run_id")
        ti_key3 = TaskInstanceKey("dag_id", "task3", "run_id")
        ti_key4 = TaskInstanceKey("dag_id", "task4", "run_id")
        ti_key5 = TaskInstanceKey("dag_id", "task5", "run_id")
        num_ranks = 4
        num_threads = 2
        num_gpus = 0

        # Ask for resources for a single task
        slurm_hook.set_task_resources(ti_key1, num_ranks, num_threads, num_gpus)
        assert ti_key1 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1])
        assert len(slots) == 1
        assert slots[0].hostname == "nid000123"
        assert [core.index for core in slots[0].cores] == list(range(8))

        # Ask for resources for two tasks
        slurm_hook.set_task_resources(ti_key2, num_ranks, num_threads, num_gpus)
        assert ti_key2 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1, ti_key2])
        assert len(slots) == 2
        assert slots[1].hostname == "nid000123"
        assert [core.index for core in slots[1].cores] == list(range(8, 16))

        # Ask for resources for three tasks
        slurm_hook.set_task_resources(ti_key3, num_ranks, num_threads, num_gpus)
        assert ti_key3 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1, ti_key2, ti_key3])
        assert len(slots) == 3
        assert slots[2].hostname == "nid000456"
        assert [core.index for core in slots[2].cores] == list(range(8))

        # Ask for resources for four tasks
        slurm_hook.set_task_resources(ti_key4, num_ranks, num_threads, num_gpus)
        assert ti_key4 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1, ti_key2, ti_key3, ti_key4])
        assert len(slots) == 4
        assert slots[3].hostname == "nid000456"
        assert [core.index for core in slots[3].cores] == list(range(8, 16))

        # Ask for resources for five tasks, but only four slots are available
        slurm_hook.set_task_resources(ti_key5, num_ranks, num_threads, num_gpus)
        assert ti_key5 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots(
            [ti_key1, ti_key2, ti_key3, ti_key4, ti_key5]
        )
        assert len(slots) == 4
        assert slots[-1].hostname == "nid000456"
        assert [core.index for core in slots[-1].cores] == list(range(8, 16))


@pytest.mark.parametrize(
    "num_gpus, gpu_occupation, gpu_list",
    [
        (2, 1.0, nullcontext(list(range(2)))),
        (4, 1.0, pytest.raises(ValueError)),
        (1, 0.25, nullcontext([0])),
        (1, 0.5, nullcontext([0])),
    ],
)
def test_find_available_slots_gpu(num_gpus, gpu_occupation, gpu_list):
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid000123"
        slurm_hook = SlurmHook()
        ti_key = TaskInstanceKey("dag_id", "task", "run_id")

        with gpu_list as gpu_expectation:
            slurm_hook.set_task_resources(ti_key, 4, 2, num_gpus)
            assert ti_key in slurm_hook.task_resource_requests
            slots = slurm_hook.find_available_slots([ti_key])
            assert len(slots) == 1
            assert slots[0].hostname == "nid000123"
            assert [gpu.index for gpu in slots[0].gpus] == gpu_expectation


@pytest.mark.parametrize(
    "num_gpus, num_tasks, gpu_ids, node_list, node_id",
    [
        ([0.5] * 2, 2, [0, 0], "nid1", ["nid1"] * 2),
        ([1, 0.5, 0.5], 3, [0, 1, 1], "nid1", ["nid1"] * 3),
        ([0.5, 1, 0.5], 3, [0, 1, 0], "nid1", ["nid1"] * 3),
        ([0.5] * 6, 6, [0, 0, 1, 1, 0, 0], "nid[1,2]", ["nid1"] * 4 + ["nid2"] * 2),
        ([0.25] * 4, 4, [0, 0, 0, 0], "nid1", ["nid1"] * 4),
        ([0.25, 0.5, 1, 0.25], 4, [0, 0, 1, 0], "nid1", ["nid1"] * 4),
    ],
)
def test_can_share_gpu(num_gpus, num_tasks, gpu_ids, node_list, node_id):
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = node_list
        slurm_hook = SlurmHook()
        ti_keys = [
            TaskInstanceKey("dag_id", f"task{i}", "run_id") for i in range(num_tasks)
        ]
        num_ranks = 2
        num_threads = 1

        for i, ti_key in enumerate(ti_keys):
            slurm_hook.set_task_resources(ti_key, num_ranks, num_threads, num_gpus[i])
        slots = slurm_hook.find_available_slots(ti_keys)
        assert len(slots) == num_tasks
        for i in range(num_tasks):
            assert slots[i].hostname == node_id[i]
            assert [gpu.index for gpu in slots[i].gpus] == [gpu_ids[i]]


@pytest.mark.parametrize(
    "num_gpus",
    [-1, -0.5, 0.1, 1.1, 1.5],
)
def test_bad_gpu_occupation_fails(num_gpus):
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid000123"
        slurm_hook = SlurmHook()
        ti_key = TaskInstanceKey("dag_id", "task", "run_id")
        with pytest.raises(ValueError):
            slurm_hook.set_task_resources(
                ti_key,
                num_ranks=4,
                num_threads=2,
                num_gpus=num_gpus,
            )


# Test that resources are allocated from alternating nodes
def test_assign_task_resources():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        ti_key1 = TaskInstanceKey("dag_id", "task1", "run_id")
        ti_key2 = TaskInstanceKey("dag_id", "task2", "run_id")
        ti_key3 = TaskInstanceKey("dag_id", "task3", "run_id")
        ti_key4 = TaskInstanceKey("dag_id", "task4", "run_id")
        ti_key5 = TaskInstanceKey("dag_id", "task5", "run_id")
        num_ranks = 4
        num_threads = 2
        num_gpus = 0

        slurm_hook.set_task_resources(ti_key1, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key1)
        assert slurm_hook.get_hostname(ti_key1) == "nid000123"
        assert slurm_hook.get_core_ids(ti_key1) == [0, 1, 2, 3, 4, 5, 6, 7]

        slurm_hook.set_task_resources(ti_key2, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key2)
        assert slurm_hook.get_hostname(ti_key2) == "nid000123"
        assert slurm_hook.get_core_ids(ti_key2) == [8, 9, 10, 11, 12, 13, 14, 15]

        slurm_hook.set_task_resources(ti_key3, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key3)
        assert slurm_hook.get_hostname(ti_key3) == "nid000456"
        assert slurm_hook.get_core_ids(ti_key3) == [0, 1, 2, 3, 4, 5, 6, 7]

        slurm_hook.set_task_resources(ti_key4, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key4)
        assert slurm_hook.get_hostname(ti_key4) == "nid000456"
        assert slurm_hook.get_core_ids(ti_key4) == [8, 9, 10, 11, 12, 13, 14, 15]

        slurm_hook.set_task_resources(ti_key5, num_ranks, num_threads, num_gpus)
        assert slurm_hook.assign_task_resources(ti_key5) == False
        assert ti_key5 not in slurm_hook.slots_dict
