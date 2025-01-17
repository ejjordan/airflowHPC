from __future__ import annotations

import os
import socket
import subprocess

from functools import cached_property
from radical.utils import get_hostlist
from airflow.hooks.base import BaseHook
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.configuration import conf
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from typing import List

from airflowHPC.hooks.resource import (
    ResourceOccupation,
    Slot,
    NodeList,
    NodeResources,
    RankRequirements,
    FREE,
)


class SlurmHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        num_tasks = os.environ.get("SLURM_TASKS_PER_NODE")
        self.tasks_per_node = (
            int(num_tasks.split("(")[0]) if num_tasks else self.cores_per_node
        )
        self.cpus_per_task = int(
            os.environ.get("SLURM_CPUS_PER_TASK", self.threads_per_core)
        )
        self.num_nodes = int(os.environ.get("SLURM_NNODES", 1))
        if "SLURM_JOB_NODELIST" in os.environ:
            self.node_names = get_hostlist(os.environ.get("SLURM_JOB_NODELIST"))
            self.num_nodes = len(self.node_names)
        elif "SLURM_JOB_ID" in os.environ:
            # needed for interactive sessions
            # sacct --noheader -X -P -oNodeList --jobs=$SLURM_JOB_ID
            result = subprocess.run(
                [
                    "sacct",
                    "--noheader",
                    "-X",
                    "-P",
                    "-oNodeList",
                    f"--jobs={os.environ['SLURM_JOB_ID']}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            nodelist = result.stdout.strip()
            self.node_names = get_hostlist(nodelist)
            self.num_nodes = len(self.node_names)
            self.log.info(
                f"SLURM_JOB_NODELIST not set, using sacct to determine node names: {self.node_names}"
            )
        else:
            if self.num_nodes > 1:
                raise ValueError("SLURM_JOB_NODELIST not set and SLURM_NNODES > 1")
            self.node_names = [socket.gethostname()]
        nodes = [
            NodeResources(
                node_index=i,
                hostname=self.node_names[i],
                cores=[
                    ResourceOccupation(index=core_idx, occupation=FREE)
                    for core_idx in range(self.tasks_per_node)
                ],
                gpus=[
                    ResourceOccupation(index=gpu_idx, occupation=FREE)
                    for gpu_idx in range(self.gpus_per_node)
                ],
                mem=self.mem_per_node,
            )
            for i in range(self.num_nodes)
        ]

        self.nodes_list = NodeList(nodes=nodes)

        super().__init__(**kwargs)
        self.task_resource_requests: dict[TaskInstanceKey, RankRequirements | None] = {}
        self.slots_dict: dict[TaskInstanceKey, Slot] = {}
        self.gpu_env_var_name = "GPU_IDS"
        self.hostname_env_var_name = "HOSTNAME"

    @cached_property
    @providers_configuration_loaded
    def cores_per_node(self) -> int:
        return int(conf.get("hpc", "cores_per_node", fallback=8))

    @cached_property
    @providers_configuration_loaded
    def gpus_per_node(self) -> int:
        return int(conf.get("hpc", "gpus_per_node", fallback=0))

    @cached_property
    @providers_configuration_loaded
    def mem_per_node(self) -> int:
        return int(conf.get("hpc", "mem_per_node", fallback=16))

    @cached_property
    @providers_configuration_loaded
    def threads_per_core(self) -> int:
        return int(conf.get("hpc", "threads_per_core", fallback=2))

    def get_core_ids(self, task_instance_key: TaskInstanceKey) -> List[int]:
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise RuntimeError(f"Resource not allocated for task {task_instance_key}")
        return [core.index for core in self.slots_dict[task_instance_key].cores]

    def get_rank_ids(self, task_instance_key: TaskInstanceKey) -> List[int]:
        return self.get_core_ids(task_instance_key)[
            :: self.task_resource_requests[task_instance_key].num_threads
        ]

    def get_gpu_ids(self, task_instance_key: TaskInstanceKey) -> List[int]:
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise RuntimeError(f"Resource not allocated for task {task_instance_key}")
        return [gpu.index for gpu in self.slots_dict[task_instance_key].gpus]

    def get_hostname(self, task_instance_key: TaskInstanceKey) -> str:
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise RuntimeError(f"Resource not allocated for task {task_instance_key}")
        return self.slots_dict[task_instance_key].hostname

    def set_task_resources(
        self,
        task_instance_key: TaskInstanceKey,
        num_ranks: int,
        num_threads: int,
        num_gpus: int | float,
    ):
        # Keep gpu_occupation out of the function signature
        if num_gpus > 0 and num_gpus < 1:
            gpu_occupation = num_gpus
            num_gpus = 1
        else:
            gpu_occupation = 1.0
        resource_request = RankRequirements(
            num_ranks=num_ranks,
            num_threads=num_threads,
            num_gpus=num_gpus,
            gpu_occupation=gpu_occupation,
        )
        # This will raise if the request cannot be satisfied
        self.nodes_list.find_slot(resource_request)
        self.task_resource_requests[task_instance_key] = resource_request

    def assign_task_resources(self, task_instance_key: TaskInstanceKey):
        assert task_instance_key in self.task_resource_requests
        resource_request = self.task_resource_requests[task_instance_key]
        slot = self.nodes_list.allocate_slot(resource_request)
        if not slot:
            return False

        self.slots_dict[task_instance_key] = slot
        self.log.debug(f"Allocated slots {slot}")
        return True

    def find_available_slots(self, task_instance_keys: List[TaskInstanceKey]):
        resource_requests: List[RankRequirements] = []
        for ti_key in task_instance_keys:
            assert ti_key in self.task_resource_requests
            resource_requests.append(self.task_resource_requests[ti_key])
        slots = self.nodes_list.find_available_slots(resource_requests)
        assert len(slots) == len(task_instance_keys)
        ret_slots = [slot for slot in slots if slot]
        tis = [task_instance_keys[i] for i, slot in enumerate(slots) if slot]
        return tis, ret_slots

    def release_task_resources(self, task_instance_key: TaskInstanceKey):
        if task_instance_key not in self.slots_dict:
            raise RuntimeError(f"Resource not allocated for task {task_instance_key}")
        self.nodes_list.release_slot(self.slots_dict[task_instance_key])
        del self.slots_dict[task_instance_key]

    def free_cores_list(self):
        return self.nodes_list.free_cores_list()
