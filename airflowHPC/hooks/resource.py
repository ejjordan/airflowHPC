from __future__ import annotations

from airflow.configuration import conf
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from dataclasses import dataclass, field, asdict
from functools import cached_property
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
    mem: int = 0
    node_index: int = 0
    hostname: str = ""


@dataclass
class NodeResources:
    index: int = 0
    name: str = ""
    cores: List[ResourceOccupation] = field(default_factory=list)
    gpus: List[ResourceOccupation] = field(default_factory=list)
    mem: int = 0


FREE = 0.0
BUSY = 1.0
DOWN = None


@dataclass
class RankRequirements:
    num_ranks: int = 1
    num_threads: int = 1
    num_gpus: int = 0
    core_occupation: float = 1.0
    gpu_occupation: float = 1.0
    mem: int = 0

    def __eq__(self, other: RankRequirements) -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __lt__(self, other: RankRequirements) -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __le__(self, other: RankRequirements) -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __gt__(self, other: RankRequirements) -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)

    def __ge__(self, other: RankRequirements) -> bool:
        if not isinstance(other, RankRequirements):
            return NotImplemented
        return asdict(self) == asdict(other)


class NodeManager:
    def __init__(self, node: NodeResources):
        self.node = node
        self.__lock__ = RLock()

    @cached_property
    @providers_configuration_loaded
    def allow_dispersed_cores(self) -> bool:
        return conf.getboolean("hpc", "allow_dispersed_cores", fallback=True)

    def _allocate_slot(self, slot: Slot) -> None:
        with self.__lock__:
            for ro in slot.cores:
                self.node.cores[ro.index].occupation += ro.occupation

            for ro in slot.gpus:
                self.node.gpus[ro.index].occupation += ro.occupation

            self.node.mem -= slot.mem

    def find_available_slots(self, rr_list: List[RankRequirements]) -> List[Slot]:
        with self.__lock__:
            slots = list()
            for rr in rr_list:
                slot = self.find_slot(rr)
                if slot:
                    slots.append(slot)
                    self._allocate_slot(slot)
                else:
                    break
            for slot in slots:
                self.deallocate_slot(slot)
            return slots

    def deallocate_slot(self, slot: Slot) -> None:
        with self.__lock__:
            for ro in slot.cores:
                self.node.cores[ro.index].occupation -= ro.occupation

            for ro in slot.gpus:
                self.node.gpus[ro.index].occupation -= ro.occupation

            self.node.mem += slot.mem

    def find_slot(self, rr: RankRequirements) -> Optional[Slot]:
        with self.__lock__:
            cores = list()
            gpus = list()

            if rr.num_ranks:
                free_cores_list = [
                    self.node.cores[i].index
                    for i in range(len(self.node.cores))
                    if self.node.cores[i].occupation == FREE
                ]
                if len(free_cores_list) < rr.num_ranks:
                    return None
                # TODO: It should be possible to just search the free_cores_list for sequential cores
                for i in range(len(free_cores_list) - rr.num_ranks + 1):
                    if all(
                        self.node.cores[i + j].occupation == FREE
                        for j in range(rr.num_ranks)
                    ):
                        cores = [
                            ResourceOccupation(
                                index=i + j, occupation=rr.core_occupation
                            )
                            for j in range(rr.num_ranks)
                        ]
                        break
                if self.allow_dispersed_cores and not cores:
                    cores = [
                        ResourceOccupation(index=i, occupation=rr.core_occupation)
                        for i in free_cores_list[: rr.num_ranks]
                    ]
                if not cores:
                    return None

            # TODO: It might make sense to ensure GPUs are contiguous, but more important would be to ensure they are on the same NUMA node
            if rr.num_gpus:
                for i in range(len(self.node.gpus) - rr.num_gpus + 1):
                    if all(
                        self.node.gpus[i + j].occupation == FREE
                        for j in range(rr.num_gpus)
                    ):
                        gpus = [
                            ResourceOccupation(
                                index=i + j, occupation=rr.gpu_occupation
                            )
                            for j in range(rr.num_gpus)
                        ]
                        break

                if not gpus:
                    return None

            if rr.mem and self.node.mem < rr.mem:
                return None

            slot = Slot(
                cores=cores,
                gpus=gpus,
                mem=rr.mem,
                node_index=self.node.index,
                hostname=self.node.name,
            )

            return slot

    def allocate_slot(self, rr: RankRequirements) -> Slot:
        slot = self.find_slot(rr)
        if slot is None:
            raise RuntimeError("could not allocate slot")
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

        self.verify()

    def verify(self) -> None:
        if not self.nodes:
            return

        self.uniform = True
        node_0 = self.nodes[0].node
        for node in self.nodes[1:]:
            if (
                node.node.cores != node_0.cores
                or node.node.gpus != node_0.gpus
                or node.node.mem != node_0.mem
            ):
                self.uniform = False
                break

        if self.uniform:
            self.cores_per_node = len(node_0.cores)
            self.gpus_per_node = len(node_0.gpus)
            self.mem_per_node = node_0.mem

        else:
            self.cores_per_node = None
            self.gpus_per_node = None
            self.lfs_per_node = None
            self.mem_per_node = None

    def _assert_rr(self, rr: RankRequirements) -> None:
        if not self.uniform:
            raise RuntimeError("verification unsupported for non-uniform nodes")

        if not rr.num_ranks:
            raise ValueError(f"invalid rank requirements: {rr}")

        requirement = self.cores_per_node / rr.num_ranks

        if rr.num_gpus:
            requirement = min(requirement, self.gpus_per_node / rr.num_gpus)

        if rr.mem:
            requirement = min(requirement, self.mem_per_node / rr.mem)

        if requirement < 1:
            raise ValueError(f"invalid rank requirements: {rr}")

    def find_slot(self, rr: RankRequirements) -> Slot | None:
        self._assert_rr(rr)

        for node in self.nodes:
            slot = node.find_slot(rr)
            if slot:
                return slot
        return None

    def find_available_slots(self, rr_list: List[RankRequirements]) -> List[Slot]:
        slots = list()
        for node in self.nodes:
            node_slots = node.find_available_slots(rr_list)
            if node_slots:
                slots.extend(node_slots)
            else:
                break
        return slots

    def allocate_slot(self, rr: RankRequirements) -> Slot | None:
        self._assert_rr(rr)

        for node in self.nodes:
            slot = node.find_slot(rr)
            if slot:
                node.allocate_slot(rr)
                return slot
        return None

    def release_slot(self, slot: Slot) -> None:
        node = self.nodes[slot.node_index]
        node.deallocate_slot(slot)
