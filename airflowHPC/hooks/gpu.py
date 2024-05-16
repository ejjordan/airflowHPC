from __future__ import annotations

import os
import radical.utils as ru
from airflow.hooks.base import BaseHook
from airflow.models.taskinstancekey import TaskInstanceKey
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from threading import RLock


@dataclass
class ResourceOccupation:
    index: int = 0
    occupation: float = 0.0


@dataclass
class Slot:
    cores: List[ResourceOccupation] = field(default_factory=list)
    gpus: List[ResourceOccupation] = field(default_factory=list)
    lfs: int = 0
    mem: int = 0
    node_index: int = 0
    node_name: str = ""


@dataclass
class NodeResources:
    index: int = 0
    name: str = ""
    cores: List[ResourceOccupation] = field(default_factory=list)
    gpus: List[ResourceOccupation] = field(default_factory=list)
    lfs: int = 0
    mem: int = 0


FREE = 0.0
BUSY = 1.0
DOWN = None


@dataclass
class RankRequirements:
    n_cores: int = 1
    core_occupation: float = 1.0
    n_gpus: int = 0
    gpu_occupation: float = 1.0
    lfs: int = 0
    mem: int = 0

    def __eq__(self, other: "RankRequirements") -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __lt__(self, other: "RankRequirements") -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __le__(self, other: "RankRequirements") -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __gt__(self, other: "RankRequirements") -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __ge__(self, other: "RankRequirements") -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)


class NodeManager:
    def __init__(self, node: NodeResources):
        self.node = node
        self.__lock__ = RLock()

    def _allocate_slot(self, slot: Slot) -> None:
        with self.__lock__:
            for ro in slot.cores:
                self.node.cores[ro.index].occupation += ro.occupation

            for ro in slot.gpus:
                self.node.gpus[ro.index].occupation += ro.occupation

            self.node.lfs -= slot.lfs
            self.node.mem -= slot.mem

    def deallocate_slot(self, slot: "Slot") -> None:
        with self.__lock__:
            for ro in slot.cores:
                self.node.cores[ro.index].occupation -= ro.occupation

            for ro in slot.gpus:
                self.node.gpus[ro.index].occupation -= ro.occupation

            self.node.lfs += slot.lfs
            self.node.mem += slot.mem

    def find_slot(self, rr: RankRequirements) -> Optional[Slot]:
        with self.__lock__:
            cores = list()
            gpus = list()

            # NOTE: the current mechanism will never use the same core or gpu
            #       multiple times for the created slot, even if the respective
            #       occupation would allow for it.
            if rr.n_cores:
                for ro in self.node.cores:
                    if ro.occupation is DOWN:
                        continue
                    if rr.core_occupation <= BUSY - ro.occupation:
                        cores.append(
                            ResourceOccupation(
                                index=ro.index, occupation=rr.core_occupation
                            )
                        )
                    if len(cores) == rr.n_cores:
                        break

                if len(cores) < rr.n_cores:
                    return None

            if rr.n_gpus:
                for ro in self.node.gpus:
                    if ro.occupation is DOWN:
                        continue
                    if rr.gpu_occupation <= BUSY - ro.occupation:
                        gpus.append(
                            ResourceOccupation(
                                index=ro.index, occupation=rr.gpu_occupation
                            )
                        )
                    if len(gpus) == rr.n_gpus:
                        break

                if len(gpus) < rr.n_gpus:
                    return None

            if rr.lfs and self.node.lfs < rr.lfs:
                return None
            if rr.mem and self.node.mem < rr.mem:
                return None

            slot = Slot(
                cores=cores,
                gpus=gpus,
                lfs=rr.lfs,
                mem=rr.mem,
                node_index=self.node.index,
                node_name=self.node.name,
            )
            self._allocate_slot(slot)

            return slot


class NodeList:
    def __init__(
        self,
        nodes: List[NodeManager],
        uniform: bool = True,
        cores_per_node: int = None,
        gpus_per_node: int = None,
        lfs_per_node: int = None,
        mem_per_node: int = None,
    ):
        self.nodes = nodes
        self.uniform = uniform
        self.cores_per_node = cores_per_node
        self.gpus_per_node = gpus_per_node
        self.lfs_per_node = lfs_per_node
        self.mem_per_node = mem_per_node

        self.__index__ = 0
        self.__last_failed_rr__ = None
        self.__last_failed_n__ = None
        self.__verified__ = False

    def verify(self) -> None:
        if not self.nodes:
            return

        self.uniform = True
        node_0 = self.nodes[0].node
        for node in self.nodes[1:]:
            if (
                node.node.cores != node_0.cores
                or node.node.gpus != node_0.gpus
                or node.node.lfs != node_0.lfs
                or node.node.mem != node_0.mem
            ):
                self.uniform = False
                break

        if self.uniform:
            self.cores_per_node = len(node_0.cores)
            self.gpus_per_node = len(node_0.gpus)
            self.lfs_per_node = node_0.lfs
            self.mem_per_node = node_0.mem

        else:
            self.cores_per_node = None
            self.gpus_per_node = None
            self.lfs_per_node = None
            self.mem_per_node = None

        self.__nodes_by_name__ = {node.node.name: node for node in self.nodes}

        self.__verified__ = True

    def _assert_rr(self, rr: RankRequirements, n_slots: int) -> None:
        if not self.__verified__:
            self.verify()

        if not self.uniform:
            raise RuntimeError("verification unsupported for non-uniform nodes")

        if not rr.n_cores:
            raise ValueError("invalid rank requirements: %s" % rr)

        ranks_per_node = self.cores_per_node / rr.n_cores

        if rr.n_gpus:
            ranks_per_node = min(ranks_per_node, self.gpus_per_node / rr.n_gpus)

        if rr.lfs:
            ranks_per_node = min(ranks_per_node, self.lfs_per_node / rr.lfs)

        if rr.mem:
            ranks_per_node = min(ranks_per_node, self.mem_per_node / rr.mem)

        if ranks_per_node < 1:
            raise ValueError("invalid rank requirements: %s" % rr)

        if n_slots > len(self.nodes) * ranks_per_node:
            raise ValueError("invalid rank requirements: %s" % rr)

    def find_slots(self, rr: RankRequirements, n_slots: int = 1) -> List[Slot] | None:
        self._assert_rr(rr, n_slots)

        if self.__last_failed_rr__:
            if self.__last_failed_rr__ >= rr and self.__last_failed_n__ >= n_slots:
                return None

        slots = list()
        count = 0
        start = self.__index__
        stop = start

        for i in range(0, len(self.nodes)):
            idx = (start + i) % len(self.nodes)
            node = self.nodes[idx]

            while True:
                count += 1
                slot = node.find_slot(rr)
                if not slot:
                    break

                slots.append(slot)
                if len(slots) == n_slots:
                    stop = idx
                    break

            if len(slots) == n_slots:
                break

        if len(slots) != n_slots:
            # free whatever we got
            for slot in slots:
                node = self.nodes[slot.node_index]
                node.deallocate_slot(slot)
            self.__last_failed_rr__ = rr
            self.__last_failed_n__ = n_slots

            return None

        self.__index__ = stop
        return slots

    def release_slots(self, slots: List[Slot]) -> None:
        for slot in slots:
            node = self.nodes[slot.node_index]
            node.deallocate_slot(slot)

        if self.__last_failed_rr__:
            self.__index__ = min([slot.node_index for slot in slots]) - 1

        self.__last_failed_rr__ = None
        self.__last_failed_n__ = None


class GPUHook(BaseHook):
    def __init__(self, **kwargs) -> None:
        rcfgs = ru.Config("radical.pilot.resource", name="*", expand=False)
        site = os.environ.get("RADICAL_PILOT_SITE", "dardel")
        platform = os.environ.get("RADICAL_PILOT_PLATFORM", "dardel_gpu")
        resource_config = ru.Config(cfg=rcfgs[site][platform])
        self.tasks_per_node = os.environ.get(
            "SLURM_TASKS_PER_NODE", resource_config.cores_per_node
        )
        self.cpus_per_task = os.environ.get(
            "SLURM_CPUS_PER_TASK", resource_config.system_architecture.smt
        )
        self.gpus_per_node = (
            resource_config.gpus_per_node
        )  # TODO: use XXX-smi to get the number of GPUs
        self.mem_per_node = os.environ.get(
            "SLURM_TASKS_PER_NODE", resource_config.mem_per_node
        )
        self.num_nodes = os.environ.get("SLURM_NNODES", 1)
        if self.num_nodes > 1:
            nodelist = os.environ.get("SLURM_JOB_NODELIST")
            if nodelist:
                self.node_names = ru.get_hostlist(nodelist)
            else:
                raise ValueError("SLURM_JOB_NODELIST not set")
        else:
            self.node_names = None
        nodes = [
            NodeManager(
                NodeResources(
                    index=i,
                    name=f"node_{i:05d}",
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

    def get_gpu_ids(self, task_instance_key: TaskInstanceKey):
        if task_instance_key not in self.slots_dict:
            self.log.info(f"Task keys {self.task_resource_requests.keys()}")
            raise ValueError(f"Resource not allocated for task {task_instance_key}")
        return [gpu.index for gpu in self.slots_dict[task_instance_key].gpus]

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
