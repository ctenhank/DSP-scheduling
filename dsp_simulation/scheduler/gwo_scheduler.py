from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.topology import Topology


class GWOScheduler(Scheduler):   
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        pass