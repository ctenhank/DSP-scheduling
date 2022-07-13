"""best fit scheduler
"""

from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology


class BestFirstScheduler(Scheduler):
    """Best First Scheduler based on the Greedy method
    
    There are several methods such as A* search ...
    Reference: https://bubble-dev.tistory.com/entry/AI-Informed-search-Greedy-Best-first-Search-A-Serach

    Args:
        Scheduler (_type_): _description_
    """
    def __init__(self):
        super().__init__()
        
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        pass