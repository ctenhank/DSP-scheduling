from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology

class RoundRobinScheduler(Scheduler):
    def __init__(self):
        super().__init__(__class__.__name__)
        
    def schedule(self, cluster: Cluster, topology: Topology) -> bool:
        print('Start Round Robin Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        #available_nodes[0].ava
        available_nodes.sort(key=lambda n: n.worker[0].capability)
        
        if not cluster.check_topology_can_be_allocated(topology):
            return False
        
        assignment = []
        for idx in range(len(topology.taskgraph.subgraph)):
            assignment.append(available_nodes[idx])
        
        cluster.assign_topology(topology, assignment)
                    
        print('Finish Round Robin Scheduling...')
        return True
