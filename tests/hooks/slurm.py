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


# TODO: fix SlurmHook.find_available_slots so this works
def test_find_available_slots():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        ti_key1 = TaskInstanceKey("dag_id", "task1", "run_id")
        ti_key2 = TaskInstanceKey("dag_id", "task2", "run_id")
        ti_key3 = TaskInstanceKey("dag_id", "task3", "run_id")
        num_ranks = 4
        num_threads = 2
        num_gpus = 0

        slurm_hook.set_task_resources(ti_key1, num_ranks, num_threads, num_gpus)
        assert ti_key1 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1])
        [
            print(f"slot1: {(slot.hostname, [core.index for core in slot.cores])}")
            for slot in slots
        ]
        print()
        slurm_hook.set_task_resources(ti_key2, num_ranks * 2, num_threads, num_gpus)
        assert ti_key2 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1, ti_key2])
        [
            print(f"slot2: {(slot.hostname, [core.index for core in slot.cores])}")
            for slot in slots
        ]
        print()
        slurm_hook.set_task_resources(ti_key3, num_ranks, num_threads, num_gpus)
        assert ti_key3 in slurm_hook.task_resource_requests
        slots = slurm_hook.find_available_slots([ti_key1, ti_key2, ti_key3])
        [
            print(f"slot3: {(slot.hostname, [core.index for core in slot.cores])}")
            for slot in slots
        ]


# Test that resources are allocated from alternating nodes
def test_assign_task_resources():
    with unittest.mock.patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "nid[000123,000456]"
        slurm_hook = SlurmHook()
        ti_key1 = TaskInstanceKey("dag_id", "task1", "run_id")
        ti_key2 = TaskInstanceKey("dag_id", "task2", "run_id")
        ti_key3 = TaskInstanceKey("dag_id", "task3", "run_id")
        ti_key4 = TaskInstanceKey("dag_id", "task4", "run_id")
        num_ranks = 4
        num_threads = 2
        num_gpus = 0
        slurm_hook.set_task_resources(ti_key1, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key1)
        assert slurm_hook.slots_dict[ti_key1].hostname == "nid000123"
        slurm_hook.set_task_resources(ti_key2, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key2)
        assert slurm_hook.slots_dict[ti_key2].hostname == "nid000123"
        slurm_hook.set_task_resources(ti_key3, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key3)
        assert slurm_hook.slots_dict[ti_key3].hostname == "nid000456"
        slurm_hook.set_task_resources(ti_key4, num_ranks, num_threads, num_gpus)
        slurm_hook.assign_task_resources(ti_key4)
        assert slurm_hook.slots_dict[ti_key4].hostname == "nid000456"
