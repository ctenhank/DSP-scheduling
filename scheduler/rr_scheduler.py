
from scheduler.scheduler import Scheduler

class RoundRobinScheduler(Scheduler):
    def schedule(self, topology, cluster):
        available_nodes = cluster.get_available_physical_node()
        print(available_nodes)
        #for node in nodes: