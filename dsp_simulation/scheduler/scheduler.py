from abc import *
from copy import deepcopy
from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.scheduler.objective import Objective
from dsp_simulation.topology.topology import Topology
import sys

class Scheduler(metaclass=ABCMeta):    
    def __init__(self, id):
        self._id = id
        
    @property
    def id(self):
        return self._id
    
    def available_workers(self, cluster: Cluster, topology: Topology) -> List[Worker]:
        """Get every available worker

        Args:
            cluster (Cluster): 
            topology (Topology): _description_

        Returns:
            List[Worker]: Available workers
        """
        ret = []
        for node in cluster.nodes:
            for worker in node.worker:
                if not worker.assigned:
                    ret.append(worker)
        return ret
        
    @abstractmethod
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        """The first called method when simulation is started

        Args:
            cluster (Cluster): _description_
            topology (Topology): _description_

        Returns:
            bool: whether fail or succeed ti schedule the given topology in the cluster
        """
        pass
    

    def reschedule(self, cluster: Cluster, topology: Topology) -> bool:
        new_cluster = deepcopy(cluster)
        assign_info = None
        for topo in new_cluster.topology_to_worker:
            if topo.id == topology.id:
                assign_info = new_cluster.topology_to_worker[topo]
        
        for subgraph in assign_info:
            worker = assign_info[subgraph]
            node = new_cluster.get_physical_node(worker.pn_id)
            if node != None:
                node.deassign(worker.id)      
                        
        topology.instantiate(3)
        
        assignment = self.schedule(new_cluster, topology)
        
        assign_info = cluster.topology_to_worker[topology]
        for subgraph in assign_info:
            worker = assign_info[subgraph]
            node = cluster.get_physical_node(worker.pn_id)
            if node != None:
                node.deassign(worker.id)
        
        cluster.topology.remove(topology)
        cluster.assign_topology(topology, assignment)
        
        return Objective.objectvie_weighted_sum(assignment)

    
    def canSchedule(self, cluster: Cluster, topology: Topology) -> bool:
        """Check
        
        Args:
            cluster (Cluster): _description_
            topology (Topology): _description_

        Returns:
            bool: _description_
        """
        nodes = cluster.get_available_physical_node()
        total_available_worker_cnt = 0
        for node in nodes:
            total_available_worker_cnt += node.available_worker_cnt
            
        if total_available_worker_cnt < len(topology.taskgraph.subgraph):
            print(f'There are no enough workers to execute the topology: required({len(topology.taskgraph.subgraph)}) < existing({total_available_worker_cnt})')
            return False
        return True

    
class MetaHueristicScheduler(Scheduler):
    def __init__(self, id):
        super().__init__(id)
    
    @abstractmethod
    def _meta_algorithm(self, cluster: Cluster, topology:Topology) -> List[PhysicalNode]:
        pass