from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology


class GWOScheduler(Scheduler):
    """Grey Wolf Optimization algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        pass