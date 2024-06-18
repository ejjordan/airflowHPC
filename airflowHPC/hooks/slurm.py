from __future__ import annotations

import os
import socket
from radical.utils import Config, get_hostlist
from airflow.hooks.base import BaseHook
from airflow.models.taskinstancekey import TaskInstanceKey
from typing import List

from airflowHPC.hooks.resource import (
    ResourceOccupation,
    Slot,
    NodeManager,
    NodeList,
    NodeResources,
    RankRequirements,
    FREE,
)


class SlurmHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        rcfgs = Config("radical.pilot.resource", name="*", expand=False)
        site = os.environ.get("RADICAL_PILOT_SITE", "dardel")
        platform = os.environ.get("RADICAL_PILOT_PLATFORM", "dardel_gpu")
        resource_config = Config(cfg=rcfgs[site][platform])
        num_tasks = os.environ.get("SLURM_TASKS_PER_NODE")
        self.tasks_per_node = (
            int(num_tasks.split("(")[0])
            if num_tasks
            else resource_config.cores_per_node
        )
        self.cpus_per_task = int(
            os.environ.get(
                "SLURM_CPUS_PER_TASK", resource_config.system_architecture.smt
            )
        )
        self.gpus_per_node = (
            resource_config.gpus_per_node
        )  # TODO: use XXX-smi to get the number of GPUs
        self.mem_per_node = resource_config.mem_per_node
        self.num_nodes = int(os.environ.get("SLURM_NNODES", 1))
        nodelist = os.environ.get("SLURM_JOB_NODELIST")
        if nodelist:
            self.node_names = get_hostlist(nodelist)
        else:
            if self.num_nodes > 1:
                raise ValueError("SLURM_JOB_NODELIST not set and SLURM_NNODES > 1")
            self.node_names = [socket.gethostname()]
        nodes = [
            NodeManager(
                NodeResources(
                    index=i,
                    name=self.node_names[i],
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
            )
            for i in range(self.num_nodes)
        ]

        self.nodes_list = NodeList(nodes=nodes)

        super().__init__(**kwargs)
        self.task_resource_requests: dict[TaskInstanceKey, RankRequirements | None] = {}
        self.slots_dict: dict[TaskInstanceKey, Slot] = {}
        self.gpu_env_var_name = "GPU_IDS"
        self.hostname_env_var_name = "HOSTNAME"

    def get_gpu_ids(self, task_instance_key: TaskInstanceKey) -> List[int]:
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise ValueError(f"Resource not allocated for task {task_instance_key}")
        return [gpu.index for gpu in self.slots_dict[task_instance_key].gpus]

    def get_node_name(self, task_instance_key: TaskInstanceKey) -> str:
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise ValueError(f"Resource not allocated for task {task_instance_key}")
        return self.slots_dict[task_instance_key].node_name

    def set_task_resources(
        self, task_instance_key: TaskInstanceKey, num_cores: int, num_gpus: int
    ):
        resource_request = RankRequirements(
            n_cores=num_cores,
            n_gpus=num_gpus,
        )
        self.task_resource_requests[task_instance_key] = resource_request

    def assign_task_resources(self, task_instance_key: TaskInstanceKey):
        if task_instance_key not in self.task_resource_requests:
            raise RuntimeError(
                f"Resource request not found fo task {task_instance_key}"
            )
        resource_request = self.task_resource_requests[task_instance_key]
        slots = self.nodes_list.find_slots(resource_request, n_slots=1)
        if not slots:
            return False
        assert len(slots) == 1
        self.slots_dict[task_instance_key] = slots[0]
        self.log.debug("Allocated slots %s", slots[0])
        return True

    def release_task_resources(self, task_instance_key: TaskInstanceKey):
        if task_instance_key not in self.slots_dict:
            raise RuntimeError(f"Resource not allocated for task {task_instance_key}")
        self.nodes_list.release_slots([self.slots_dict[task_instance_key]])
        del self.slots_dict[task_instance_key]