import pytest
from airflowHPC.hooks.resource import (
    NodeList,
    NodeResources,
    RankRequirements,
    ResourceOccupation,
    FREE,
)


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
    num_nodes = 1
    node_names = ["node1"]
    tasks_per_node = 16
    gpus_per_node = 0
    mem_per_node = 64
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
        for i in range(num_nodes)
    ]
    nodes_list = NodeList(nodes=nodes)
    slots = nodes_list.find_available_slots(rr_list)
    assert len(slots) == expected_result[0]
    for i in range(expected_result[0]):
        assert slots[i].cores[0].index == expected_result[i + 1]
