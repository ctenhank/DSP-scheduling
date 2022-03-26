from abc import abstractmethod
from component.cluster import Cluster

from component.topology import Topology

class Scheduler:
    def __init__(self):
        pass
    
    @abstractmethod
    def schedule(self, topology: Topology, cluster: Cluster):
        pass
    
    