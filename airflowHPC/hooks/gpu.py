from __future__ import annotations

import os
import radical.utils as ru
import radical.pilot as rp
from airflow.hooks.base import BaseHook
from airflow.models.taskinstancekey import TaskInstanceKey
from radical.pilot import Slot


class GPUHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        rcfgs = ru.Config("radical.pilot.resource", name="*", expand=False)
        site = os.environ.get("RADICAL_PILOT_SITE", "dardel")
        platform = os.environ.get("RADICAL_PILOT_PLATFORM", "dardel_gpu")
        resource_config = ru.Config(cfg=rcfgs[site][platform])
        self.cores_per_node = resource_config.cores_per_node
        self.gpus_per_node = resource_config.gpus_per_node
        self.mem_per_node = resource_config.mem_per_node
        self.num_nodes = os.environ.get("RADICAL_NUM_NODES", 1)
        nodes = [
            {
                "index": i,
                "name": "node_%05d" % i,
                "cores": [
                    rp.ResourceOccupation(index=core_idx, occupation=rp.FREE)
                    for core_idx in range(self.cores_per_node)
                ],
                "gpus": [
                    rp.ResourceOccupation(index=gpu_idx, occupation=rp.FREE)
                    for gpu_idx in range(self.gpus_per_node)
                ],
                "mem": self.mem_per_node,
            }
            for i in range(self.num_nodes)
        ]

        self.nodes_list = rp.NodeList(nodes=[rp.NodeResources(ni) for ni in nodes])

        super().__init__(**kwargs)
        self.task_resource_requests: dict[
            TaskInstanceKey, rp.RankRequirements | None
        ] = {}
        self.slots_dict: dict[TaskInstanceKey, Slot] = {}
        self.gpu_env_var_name = "GPU_IDS"

    def get_gpu_ids(self, task_instance_key: TaskInstanceKey):
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise ValueError(f"Resource not allocated for task {task_instance_key}")
        return [gpu.index for gpu in self.slots_dict[task_instance_key].gpus]

    def set_task_resources(
        self, task_instance_key: TaskInstanceKey, num_cores: int, num_gpus: int
    ):
        resource_request = rp.RankRequirements(
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
