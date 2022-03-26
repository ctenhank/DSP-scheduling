from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology
import random as rd


class RandomScheduler(Scheduler):   
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        print('Start Random Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        
        if not cluster.check_topology_can_be_allocated(topology):
            return False
        
        assignment = []
        for _ in topology.subgraph:
            node = rd.choice(available_nodes)
            while node.available_worker_cnt > 0:
                node = rd.choice(available_nodes)
            assignment.append(node)
        
        cluster.assign_topology(topology, assignment)
                    
        print('Finish Random Scheduling...')
        return True