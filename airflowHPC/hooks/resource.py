from __future__ import annotations

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
    num_cores: int = 1
    num_gpus: int = 0
    core_occupation: float = 1.0
    gpu_occupation: float = 1.0
    lfs: int = 0
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

    def _allocate_slot(self, slot: Slot) -> None:
        with self.__lock__:
            for ro in slot.cores:
                self.node.cores[ro.index].occupation += ro.occupation

            for ro in slot.gpus:
                self.node.gpus[ro.index].occupation += ro.occupation

            self.node.lfs -= slot.lfs
            self.node.mem -= slot.mem

    def deallocate_slot(self, slot: Slot) -> None:
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
            if rr.num_cores:
                for ro in self.node.cores:
                    if ro.occupation is DOWN:
                        continue
                    if rr.core_occupation <= BUSY - ro.occupation:
                        cores.append(
                            ResourceOccupation(
                                index=ro.index, occupation=rr.core_occupation
                            )
                        )
                    if len(cores) == rr.num_cores:
                        break

                if len(cores) < rr.num_cores:
                    return None

            if rr.num_gpus:
                for ro in self.node.gpus:
                    if ro.occupation is DOWN:
                        continue
                    if rr.gpu_occupation <= BUSY - ro.occupation:
                        gpus.append(
                            ResourceOccupation(
                                index=ro.index, occupation=rr.gpu_occupation
                            )
                        )
                    if len(gpus) == rr.num_gpus:
                        break

                if len(gpus) < rr.num_gpus:
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

    def _assert_rr(self, rr: RankRequirements) -> None:
        if not self.uniform:
            raise RuntimeError("verification unsupported for non-uniform nodes")

        if not rr.num_cores:
            raise ValueError(f"invalid rank requirements: {rr}")

        requirement = self.cores_per_node / rr.num_cores

        if rr.num_gpus:
            requirement = min(requirement, self.gpus_per_node / rr.num_gpus)

        if rr.lfs:
            requirement = min(requirement, self.lfs_per_node / rr.lfs)

        if rr.mem:
            requirement = min(requirement, self.mem_per_node / rr.mem)

        if requirement < 1:
            raise ValueError(f"invalid rank requirements: {rr}")

    def find_slots(self, rr: RankRequirements) -> Slot | None:
        self._assert_rr(rr)

        for node in self.nodes:
            slot = node.find_slot(rr)
            if slot:
                return slot
        return None

    def release_slots(self, slot: Slot) -> None:
        node = self.nodes[slot.node_index]
        node.deallocate_slot(slot)
