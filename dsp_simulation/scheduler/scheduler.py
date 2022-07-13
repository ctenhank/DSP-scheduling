from abc import *
from copy import deepcopy
from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.topology.topology import Topology
import numpy as np
from datetime import datetime
import time

class Scheduler(metaclass=ABCMeta):    
    def __init__(self, id):
        self._id = id
        
    @property
    def id(self):
        return self._id
    
    def _z_score(self, fitness_arr, idx):
        return abs(fitness_arr[idx] - np.mean(fitness_arr)) / np.std(fitness_arr)
    
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
        #print(cluster.get)
        cnt = 0
        for node in cluster.get_available_physical_node():
            for worker in node.get_available_worker():
                cnt += 1
        cnt2 = 0
        for node in new_cluster.get_available_physical_node():
            for worker in node.get_available_worker():
                cnt2 += 1
                
        for node in new_cluster.nodes:
            for worker in node.worker:
                node.deassign(worker.id)
                if worker.assigned == True:
                    print(f'{worker.id}: {worker.assigned}')
        cnt3 = 0
        for node in new_cluster.get_available_physical_node():
            for worker in node.get_available_worker():
                cnt3 += 1
                
        topology.instantiate(3)
        stime = datetime.now()
        stime2 = time.time_ns()
        assignment = self.schedule(new_cluster, topology)
        etime2 = time.time_ns()
        etime = datetime.now()
        return new_cluster, assignment, (etime - stime), (etime2 - stime2)

    
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