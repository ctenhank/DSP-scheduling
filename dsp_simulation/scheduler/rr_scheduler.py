from dsp_simulation.scheduler.scheduler import Scheduler

class RoundRobinScheduler(Scheduler):
    def schedule(self, topology, cluster) -> bool:
        print('Start Round Robin Scheduling...')
        
        available_nodes = cluster.get_available_physical_node()
        available_nodes.sort(key=lambda n: n.worker[0].capability)
        
        if not cluster.check_topology_can_be_allocated(topology):
            return False
        
        assignment = []
        for idx in range(len(topology.subgraph)):
            assignment.append(available_nodes[idx])
        
        cluster.assign_topology(topology, assignment)
                    
        print('Finish Round Robin Scheduling...')
        return True