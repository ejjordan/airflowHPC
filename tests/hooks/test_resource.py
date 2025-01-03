import pytest
from contextlib import nullcontext
from airflowHPC.hooks.resource import (
    NodeList,
    NodeResources,
    RankRequirements,
    ResourceOccupation,
    FREE,
    BUSY,
)


def get_node(
    node_names: list[str],
    tasks_per_node: int = 16,
    gpus_per_node: int = 0,
    mem_per_node: int = 64,
) -> NodeList:
    nodes = [
        NodeResources(
            node_index=i,
            hostname=node_names[i],
            cores=[
                ResourceOccupation(index=core_idx, occupation=FREE)
                for core_idx in range(tasks_per_node)
            ],
            gpus=[
                ResourceOccupation(index=gpu_idx, occupation=FREE)
                for gpu_idx in range(gpus_per_node)
            ],
            mem=mem_per_node,
        )
        for i in range(len(node_names))
    ]
    return NodeList(nodes=nodes)


@pytest.mark.parametrize(
    "rr_list, expected_result",
    [
        ([RankRequirements(num_ranks=4, num_threads=2)] * 2, [2, 0, 8]),
        ([RankRequirements(num_ranks=2, num_threads=4)] * 2, [2, 0, 8]),
        ([RankRequirements(num_ranks=2, num_threads=2)] * 4, [4, 0, 4, 8, 12]),
        (
            [
                RankRequirements(num_ranks=3, num_threads=1),
                RankRequirements(num_ranks=4, num_threads=2),
                RankRequirements(num_ranks=1, num_threads=1),
            ],
            [3, 0, 3, 11],
        ),
    ],
)
def test_find_available_slots(rr_list, expected_result):
    node_names = ["node1"]
    nodes_list = get_node(node_names)
    slots = nodes_list.find_available_slots(rr_list)
    assert len(slots) == expected_result[0]
    for i in range(expected_result[0]):
        assert slots[i].cores[0].index == expected_result[i + 1]


@pytest.mark.parametrize(
    "rr_list, expected_result",
    [
        ([RankRequirements(num_ranks=2, num_threads=2)] * 2, [2, 6, 10]),
        (
            [
                RankRequirements(num_ranks=2, num_threads=4),
                RankRequirements(num_ranks=1),
                RankRequirements(num_ranks=1),
            ],
            [3, 6, 0, 1],
        ),
    ],
)
def test_find_available_slots_busy(rr_list, expected_result):
    node_names = ["node1"]
    nodes_list = get_node(node_names)
    for i in [2, 3, 4, 5]:
        nodes_list.nodes[0].cores[i].occupation = BUSY
    slots = nodes_list.find_available_slots(rr_list)
    assert len(slots) == expected_result[0]
    for i in range(expected_result[0]):
        assert slots[i].cores[0].index == expected_result[i + 1]


def test_allocate_slot():
    node_names = ["node1"]
    nodes_list = get_node(node_names)
    num_ranks = 2
    num_threads = 2
    rr = RankRequirements(num_ranks=num_ranks, num_threads=num_threads)
    slot = nodes_list.allocate_slot(rr)
    assert slot is not None
    assert len(slot.cores) == num_ranks * num_threads
    assert [slot.cores[i].occupation == BUSY for i in range(num_ranks * num_threads)]
    assert slot.mem == 0  # Didn't request memory
    assert slot.node_index == 0
    assert slot.hostname == "node1"
    assert len(slot.gpus) == 0
    assert slot.gpus == []
    assert [
        nodes_list.nodes[0].cores[0].occupation == BUSY
        for i in range(num_ranks * num_threads)
    ]
    assert [core.occupation == FREE for core in nodes_list.nodes[0].cores[4:]]


def test_allocate_slot_multinode():
    node_names = ["node1", "node2"]
    nodes_list = get_node(node_names)
    num_ranks = 3
    num_threads = 2
    rr_list = [RankRequirements(num_ranks=num_ranks, num_threads=num_threads)] * 6
    slots = [nodes_list.allocate_slot(rr) for rr in rr_list]

    assert len(slots) == 6
    assert [
        [core.occupation == BUSY for core in nodes_list.nodes[i].cores[:12]]
        for i in range(len(node_names))
    ]
    assert [
        [core.occupation == FREE for core in nodes_list.nodes[i].cores[12:]]
        for i in range(len(node_names))
    ]


def test_release_slot():
    node_names = ["node1"]
    nodes_list = get_node(node_names)
    num_ranks = 2
    num_threads = 2
    rr = RankRequirements(num_ranks=num_ranks, num_threads=num_threads)
    slot = nodes_list.allocate_slot(rr)
    assert [slot.cores[i].occupation == BUSY for i in range(num_ranks * num_threads)]
    assert [
        nodes_list.nodes[0].cores[0].occupation == BUSY
        for i in range(num_ranks * num_threads)
    ]
    nodes_list.release_slot(slot)
    assert [
        nodes_list.nodes[0].cores[i].occupation == FREE
        for i in range(len(nodes_list.nodes[0].cores))
    ]


@pytest.mark.parametrize(
    "num_gpus, gpu_occupation, expected_result",
    [
        (2, 1, nullcontext(2)),
        (1, 0.5, nullcontext(1)),
        (2, 0.5, pytest.raises(ValueError)),
        (1, 0.25, nullcontext(1)),
        (1, 0.1, pytest.raises(ValueError)),
        (1, 0, pytest.raises(ValueError)),
        (0, 0, pytest.raises(ValueError)),
        (-1, 1.0, pytest.raises(ValueError)),
    ],
)
def test_resource_req(num_gpus, gpu_occupation, expected_result):
    with expected_result as expected_gpus:
        rr = RankRequirements(
            num_ranks=4, num_threads=2, num_gpus=num_gpus, gpu_occupation=gpu_occupation
        )
        assert rr.num_gpus == expected_gpus
