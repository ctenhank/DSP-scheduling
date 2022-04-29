from abc import *
from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.topology.topology import Topology
import sys

class Scheduler(metaclass=ABCMeta):    
    def __init__(self, id):
        self._id = id
        
    @property
    def id(self):
        return self._id
        
    @abstractmethod
    def schedule(self, cluster: Cluster, topology: Topology) -> bool:
        pass
    
    def canSchedule(self, cluster: Cluster, topology: Topology) -> bool:
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