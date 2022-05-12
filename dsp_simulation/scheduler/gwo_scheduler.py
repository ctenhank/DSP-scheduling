from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.scheduler import MetaHueristicScheduler
from dsp_simulation.topology.topology import Topology
import random as rd

class Wolf:
    def __init__(self, fitness, dim, seed, cluster: Cluster, topology: Topology):
        self._rnd = rd.seed(seed)
        #self._position = [0.0 for i in range(dim)]
        
        
        #for i in range(dim):
        #    self._position[i] = 
        self._position = []
        self.fitness = fitness()
        pass

class GWOScheduler(MetaHueristicScheduler):
    """Grey Wolf Optimization algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """
    def __init__(self, num_wolf: int):
        super().__init__(__class__.__name__)
        self._num_wolf = num_wolf
        
    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        
        population = [Wolf(fitness=None, dim=None, seed=i) for i in range(self._num_wolf) ]
        population = sorted(population, key=lambda wolf: wolf.fitness)
        pass
        
    def schedule(self, cluster: Cluster, topology: Topology) -> bool:
        if not self.canSchedule(cluster, topology):
            return False
        best = self._meta_algorithm(cluster, topology)
        
        if best is None:
            return False
        
        cluster.assign_topology(topology, best)
        return True