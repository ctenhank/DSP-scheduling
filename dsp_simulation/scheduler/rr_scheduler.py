from dsp_simulation.scheduler.scheduler import Scheduler

class RoundRobinScheduler(Scheduler):
    def schedule(self, topology, cluster) -> bool:
        
        available_nodes = cluster.get_available_physical_node()
        available_nodes.sort(key=lambda n: n.worker[0].capability)
        
        # topology를 할당할 수 있는가?
        if not cluster.check_topology_can_be_allocated(topology):
            print('There are no enough resource and workers to run the topology in this cluster.')
            return False
            
        for subgraph in topology.subgraph:
            for node in available_nodes:
                print(node)
                if node.assign_topology(subgraph):
                    print(f'Assigned {subgraph} on {node}')
                    break
                    
        #for node in available_nodes:
        #    print(f'{node}, {node.worker[0].capability}')
            
        return True
            #print(f'{node.id}: {node.worker[0].capability}')
        #print(available_nodes)
        #for node in nodes: