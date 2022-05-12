from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology
import random as rd


class RandomScheduler(Scheduler):   
    def __init__(self):
        super().__init__(__class__.__name__)
        
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        print('Start Random Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        
        if not cluster.check_topology_can_be_allocated(topology):
            return None
        
        assignment = []
        available_worker_cnt = {}
        len_nodes = len(available_nodes)
        len_subgraph = len(topology.taskgraph.subgraph)
        
        for node in available_nodes: 
            available_worker_cnt[node.id] = node.available_worker_cnt
        
        cnt = 0
        while cnt < len_subgraph:
            node = rd.choice(available_nodes)
            while available_worker_cnt[node.id] < 1:
                node = rd.choice(available_nodes)
            assignment.append(node)
            available_worker_cnt[node.id] -= 1
        
            cnt += 1

        #for _ in topology.taskgraph.subgraph:
        #    node = rd.choice(available_nodes)
        #    while node.available_worker_cnt < 1:
        #        node = rd.choice(available_nodes)
        #    assignment.append(node)
        #    node.available_worker_cnt = node.available_worker_cnt-1
        print('Finish Random Scheduling...')
        return assignment
        cluster.assign_topology(topology, assignment)
                    
        
        return True