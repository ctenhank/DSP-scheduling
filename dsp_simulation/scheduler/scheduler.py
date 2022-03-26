from abc import *
from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.topology.topology import Topology
import sys

class Scheduler(metaclass=ABCMeta):   
    @abstractmethod
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        pass
    
    def canSchedule(self, topology: Topology, cluster: Cluster) -> bool:
        nodes = cluster.get_available_physical_node()
        total_available_worker_cnt = 0
        for node in nodes:
            total_available_worker_cnt += node.available_worker_cnt
            
        if total_available_worker_cnt < len(topology.subgraph):
            print(f'There are no enough workers to execute the topology: required({len(topology.subgraph)}) < existing({total_available_worker_cnt})')
            return False
        return True
    
class MetaHueristicScheduler(Scheduler):
    def __init__(self, max_iter):
        self._num_iteration = 100
    
    @abstractmethod
    def _meta_algorithm(self, topology:Topology, cluster: Cluster) -> List[PhysicalNode]:
        pass