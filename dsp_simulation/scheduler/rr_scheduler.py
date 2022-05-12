from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology

class RoundRobinScheduler(Scheduler):
    def __init__(self):
        super().__init__(__class__.__name__)
        
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        print('Start Round Robin Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        available_worker_cnt = {}
        len_nodes = len(available_nodes)
        len_subgraph = len(topology.taskgraph.subgraph)
        #available_nodes[0].ava
        #available_nodes.sort(key=lambda n: n.worker[0].capability)
        
        if not cluster.check_topology_can_be_allocated(topology):
            return None
        
        for node in available_nodes: 
            available_worker_cnt[node.id] = node.available_worker_cnt
        
        assignment = []
        print(f'Subgraph #: {len(topology.taskgraph.subgraph)}, Node #: {len(available_nodes)}')
        cnt = 0
        idx = 0
        while cnt < len_subgraph:
            node = available_nodes[idx % len_nodes]
            while available_worker_cnt[node.id] < 1:
                idx += 1
                node = available_nodes[idx % len_nodes]
            assignment.append(node)
            available_worker_cnt[node.id] -= 1
        
            idx += 1
            cnt += 1
            
        return assignment
        #cluster.assign_topology(topology, assignment)
                    
        #print('Finish Round Robin Scheduling...')
        #return True
