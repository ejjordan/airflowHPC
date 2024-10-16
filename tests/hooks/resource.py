from airflowHPC.hooks.resource import (
    NodeList,
    NodeResources,
    RankRequirements,
    ResourceOccupation,
    FREE,
)


def test_find_available_slots():
    rr_list = [RankRequirements(num_ranks=4, num_threads=2)] * 2
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
    assert len(slots) == 2
    assert slots[0].cores[0].index == 0
    assert slots[1].cores[0].index == 8
