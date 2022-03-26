from typing import List
import random as rd

from dsp_simulation.cluster.worker import Worker
from dsp_simulation.topology.subtopology import SubTopology

class PhysicalNode:
    """Physical node running the topology in the cluster.
    It has computing capacity, one of [50.0, 100.0, 200.0, 400.0], is based on one core of the Intel(R) Core(TM) i7-8700K CPU @ 3.70GHz.
    Each physical node can has multiple workers.
    
    There are a few class member such as CNT, FAILURE, CAPABILITY, MINIMAL_REQUIRED_RESOURCES_PER_WORKER.
    Class members:
        CNT This member is related to the PhysicalNode's ID. The id of nodes is increased incrementally.
        FAILURE Failure rate of physical nodem, default is to 0.025
        CAPABILITY Computing capability, each physical nodes is allocated randomly one of the [50, 100.0, 200.0, 400.0]
        MINIMAL_REQUIRED_RESOURCES_PER_WORKER We assume that each worker has three tasks at least. So that's the 25.0.
    """
    CNT = 0
    FAILURE = 0.025
    CAPABILITY = [50.0, 100.0, 200.0, 400.0]
    MINIMAL_REQUIRED_RESOURCES_PER_WORKER = 25.0
    
    def __init__(self, max_worker, rack=None):
        self.__id = 'node-' + str(PhysicalNode.CNT)
        PhysicalNode.CNT += 1
        self._cap = rd.choice(PhysicalNode.CAPABILITY)
        self._remains = self._cap
        self._rack: List[str] = rack
        self._worker: List[Worker] = self._get_workers(max_worker)
        self._available_worker: List[bool] = [True for _ in range(len(self._worker))]
        self._available_worker_cnt: int = len(self._worker)
        self._failure = PhysicalNode.FAILURE

    def __str__(self):
        ret = f'{self.__id}, {self._cap}, {self._rack}, {self._remains}, {len(self._worker)}'
        return ret
    
    @property
    def id(self):
        return self.__id
    
    @property
    def cap(self):
        return self._cap
    
    @property
    def remains(self):
        return self._remains
    
    @property
    def rack(self):
        return self._rack
    
    @rack.setter
    def rack(self, rack):
        self._rack = rack
    
    @property
    def worker(self):
        return self._worker
    
    @property
    def available_worker_cnt(self):
        return self._available_worker_cnt
    
    
    def _get_workers(self, max_worker: int):
        """generate and get workers based on max_worker or the amount of resources

        Args:
            max_worker (int): the maximum number of worker. It can be changed depending on the amount of resources.

        Returns:
            List[Worker]: List of created Workers
        """
        # Resource에 따라서 reconfigure
        internal_max_worker = int(self._cap / PhysicalNode.MINIMAL_REQUIRED_RESOURCES_PER_WORKER)
        if max_worker > internal_max_worker:
            max_worker = internal_max_worker
            
        num_worker = rd.randint(1, max_worker)
        
        worker_resource = self._cap / num_worker
        return [Worker(worker_resource) for _ in range(num_worker)]
    
    
    def get_available_worker(self) -> List[Worker]:
        """To execute any topology in this physical machine, we must need enough workers.

        Returns:
            List[Worker]: List of available workers
        """
        ret = []
        for idx, available in enumerate(self._available_worker):
            if available:
                ret.append(self._worker[idx])
            
        #for idx, worker in enumerate(self.__worker):
        #    if not worker.assigned:
        #        availale_worker.append(idx)
        return ret
        
    
    def assign(self, graph: SubTopology):
        """Assign subgraph of the topology in this physical machine if there is available worker.

        Args:
            bool: 
        """
        
        if len(self.get_available_worker()) <= 0:
            return False
        
        selected = 0
        for idx, worker in enumerate(self._worker):
            if not worker.assigned:
                selected = idx
        
        self._available_worker[selected] = False
        self._available_worker_cnt -= 1
        
        self._worker[selected].assigned = True
        self._worker[selected].assign(graph)
        
        for task in graph.tasks:
            task.assigned_cap = self._worker[selected].capability
            task.update_latency_generator()
            
        print(f'Assigned {str(graph)} on {self._worker[selected].id} of {self.__id}')
        return True
    
    def deassign(self, idx):
        """Deassign subgraph in the given worker

        Args:
            idx (int): Index of the worker
        """
        self._available_worker[idx] = True
        self._available_worker_cnt += 1
        self._worker[idx].deassign()
        print(f'Deassigned {self._worker[idx].id} of {self.__id}')