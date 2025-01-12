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
    occupation: float = 1.0

    def __repr__(self) -> str:
        return f"{self.index}:{self.occupation}"


@dataclass
class NodeResources:
    """For keeping track of resources on a node."""

    cores: List[ResourceOccupation] = field(default_factory=list)
    gpus: List[ResourceOccupation] = field(default_factory=list)
    mem: int = 0
    node_index: int = 0
    hostname: str = ""


class Slot(NodeResources):
    """For keeping track of resources allocated to a task."""

    pass


FREE = 0.0
BUSY = 1.0
DOWN = None


@dataclass
class RankRequirements:
    num_ranks: int
    num_threads: int
    num_gpus: int
    gpu_occupation: float
    mem: int = 0
    _num_ranks: int = field(default=1, init=False, repr=False)
    _num_threads: int = field(default=1, init=False, repr=False)
    _gpu_occupation: float = field(default=1.0, init=False, repr=False)
    _num_gpus: int = field(default=0, init=False, repr=False)
    core_occupation: float = field(default=1.0, init=False, repr=False)

    @property
    def num_ranks(self) -> int:
        return self._num_ranks

    @num_ranks.setter
    def num_ranks(self, value: int) -> None:
        if type(value) is property:
            # Use the default value if not specified
            value = RankRequirements._num_ranks
        check_value = float(value)
        if check_value <= 0 or check_value.as_integer_ratio()[1] != 1:
            raise ValueError(f"num_threads: {value} must be a positive integer")
        self._num_ranks = int(value)

    @property
    def num_threads(self) -> int:
        return self._num_threads

    @num_threads.setter
    def num_threads(self, value: int) -> None:
        if type(value) is property:
            # Use the default value if not specified
            value = RankRequirements._num_threads
        check_value = float(value)
        if check_value <= 0 or check_value.as_integer_ratio()[1] != 1:
            raise ValueError(f"num_threads: {value} must be a positive integer")
        self._num_threads = int(value)

    @property
    def num_gpus(self) -> int:
        return self._num_gpus

    @num_gpus.setter
    def num_gpus(self, value: int) -> None:
        if type(value) is property:
            # Use the default value if not specified
            value = RankRequirements._num_gpus
        check_value = float(value)
        if check_value < 0 or check_value.as_integer_ratio()[1] != 1:
            raise ValueError(f"num_gpus: {value} must be a non-negative integer")
        self._num_gpus = int(value)

    @property
    def gpu_occupation(self) -> float:
        return self._gpu_occupation

    @gpu_occupation.setter
    def gpu_occupation(self, value: float) -> None:
        if type(value) is property:
            # Use the default value if not specified
            value = RankRequirements._gpu_occupation
        value = float(value)
        allowed_values = [0.25, 0.5, 1.0]
        if value not in allowed_values:
            raise ValueError(f"gpu_occupation: {value} not in {allowed_values}")
        if self.num_gpus > 1 and value != 1.0:
            msg = f"gpu_occupation is {value}, which is only allowed for a single GPU. "
            msg += f"It must be 1.0 for {self.num_gpus} GPUs"
            raise ValueError(msg)
        self._gpu_occupation = value

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


class NodeList:
    def __init__(
        self,
        nodes: List[NodeResources],
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

        self.__lock__ = RLock()

        self.verify()

    @cached_property
    @providers_configuration_loaded
    def allow_dispersed_cores(self) -> bool:
        return conf.getboolean("hpc", "allow_dispersed_cores", fallback=True)

    def verify(self) -> None:
        if not self.nodes:
            return

        self.uniform = True
        node_0 = self.nodes[0]
        for node in self.nodes[1:]:
            if (
                node.cores != node_0.cores
                or node.gpus != node_0.gpus
                or node.mem != node_0.mem
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

        requirement = self.cores_per_node / (rr.num_ranks * rr.num_threads)

        if rr.num_gpus:
            requirement = min(requirement, self.gpus_per_node / rr.num_gpus)

        if rr.mem:
            requirement = min(requirement, self.mem_per_node / rr.mem)

        if requirement < 1:
            raise ValueError(f"invalid rank requirements: {rr}")

    def free_cores_list(self):
        free_cores = {}
        for node in self.nodes:
            free_cores[node.hostname] = len(
                [
                    node.cores[i].index
                    for i in range(len(node.cores))
                    if node.cores[i].occupation == FREE
                ]
            )
        return free_cores

    def find_slot(self, rr: RankRequirements) -> Slot | None:
        self._assert_rr(rr)

        for node in self.nodes:
            slot = self._find_slot(node, rr)
            if slot:
                return slot
        return None

    def _find_slot(self, node: NodeResources, rr: RankRequirements) -> Optional[Slot]:
        with self.__lock__:
            cores = list()
            gpus = list()

            if rr.num_ranks:
                free_cores_list = [
                    node.cores[i].index
                    for i in range(len(node.cores))
                    if node.cores[i].occupation == FREE
                ]
                if len(free_cores_list) < rr.num_ranks * rr.num_threads:
                    return None
                # TODO: It should be possible to just search the free_cores_list for sequential cores
                for i in range(len(node.cores) - rr.num_ranks * rr.num_threads + 1):
                    if all(
                        node.cores[i + j].occupation == FREE
                        for j in range(rr.num_ranks * rr.num_threads)
                    ):
                        cores = [
                            ResourceOccupation(
                                index=i + j, occupation=rr.core_occupation
                            )
                            for j in range(rr.num_ranks * rr.num_threads)
                        ]
                        break
                if self.allow_dispersed_cores and not cores:
                    cores = [
                        ResourceOccupation(index=i, occupation=rr.core_occupation)
                        for i in free_cores_list[: rr.num_ranks * rr.num_threads]
                    ]
                if not cores:
                    return None

            # TODO: It might make sense to ensure GPUs are contiguous, but more important would be to ensure they are on the same NUMA node
            if rr.num_gpus:
                for i in range(len(node.gpus) - rr.num_gpus + 1):
                    if any(
                        1 - node.gpus[i + j].occupation >= rr.gpu_occupation
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

            if rr.mem and node.mem < rr.mem:
                return None

            slot = Slot(
                cores=cores,
                gpus=gpus,
                mem=rr.mem,
                node_index=node.node_index,
                hostname=node.hostname,
            )

            return slot

    def find_available_slots(
        self, rr_list: List[RankRequirements]
    ) -> List[Slot | None]:
        with self.__lock__:
            slots = list()
            for rr in rr_list:
                alloc_slot = None
                for node in self.nodes:
                    if self._find_slot(node, rr):
                        alloc_slot = self._allocate_slot(node, rr)
                        break
                slots.append(alloc_slot)

            for slot in slots:
                if slot:
                    self.release_slot(slot)
            return slots

    def allocate_slot(self, rr: RankRequirements) -> Slot | None:
        self._assert_rr(rr)
        for node in self.nodes:
            if self._find_slot(node, rr):
                alloc_slot = self._allocate_slot(node, rr)
                return alloc_slot
        return None

    def _allocate_slot(self, node: NodeResources, rr: RankRequirements) -> Slot:
        slot = self._find_slot(node, rr)
        if slot is None:
            raise RuntimeError("could not allocate slot")
        with self.__lock__:
            for ro in slot.cores:
                node.cores[ro.index].occupation += ro.occupation

            for ro in slot.gpus:
                node.gpus[ro.index].occupation += ro.occupation

            node.mem -= slot.mem
        return slot

    def release_slot(self, slot: Slot) -> None:
        node = self.nodes[slot.node_index]
        with self.__lock__:
            for ro in slot.cores:
                node.cores[ro.index].occupation -= ro.occupation

            for ro in slot.gpus:
                node.gpus[ro.index].occupation -= ro.occupation

            node.mem += slot.mem
