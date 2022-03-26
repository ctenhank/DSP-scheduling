from abc import abstractmethod
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.topology.topology import Topology

class Scheduler:   
    @abstractmethod
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        pass
    
    