from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology
import random as rd


class RandomScheduler(Scheduler):   
    def __init__(self):
        super().__init__(__class__.__name__)
        
    def schedule(self, cluster: Cluster, topology: Topology) -> bool:
        print('Start Random Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        
        if not cluster.check_topology_can_be_allocated(topology):
            return False
        
        assignment = []
        for _ in topology.taskgraph.subgraph:
            node = rd.choice(available_nodes)
            while node.available_worker_cnt < 1:
                node = rd.choice(available_nodes)
            assignment.append(node)
            node.available_worker_cnt = node.available_worker_cnt-1
        
        cluster.assign_topology(topology, assignment)
                    
        print('Finish Random Scheduling...')
        return True