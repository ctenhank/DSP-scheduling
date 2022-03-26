from typing import List
import random as rd

from dsp_simulation.cluster.worker import Worker

class PhysicalNode:
    CNT = 0
    FAILURE = 0.025
    CAPABILITY = [50.0, 100.0, 200.0, 400.0]
    MINIMAL_REQUIRED_RESOURCES_FOR_WORKER = 25.0
    
    def __init__(self, max_worker, rack=None):
        self.__id = 'node-' + str(PhysicalNode.CNT)
        PhysicalNode.CNT += 1
        self.__cap = rd.choice(PhysicalNode.CAPABILITY)
        self.__remains = self.__cap
        self.__rack = rack
        self.__worker = self._get_workers(max_worker)
        self.__failure = PhysicalNode.FAILURE

    def __str__(self):
        ret = f'{self.__id}, {self.__cap}, {self.__rack}, {self.__remains}, {len(self.__worker)}'
        return ret
    
    @property
    def id(self):
        return self.__id
    
    @property
    def cap(self):
        return self.__cap
    
    @property
    def remains(self):
        return self.__remains
    
    @property
    def rack(self):
        return self.__rack
    
    @rack.setter
    def rack(self, rack):
        self.__rack = rack
    
    @property
    def worker(self):
        return self.__worker
    
    
    
    def _get_workers(self, max_worker):
        # Resource에 따라서 reconfigure
        internal_max_worker = int(self.__cap / PhysicalNode.MINIMAL_REQUIRED_RESOURCES_FOR_WORKER)
        if max_worker > internal_max_worker:
            max_worker = internal_max_worker
            
        num_worker = rd.randint(1, max_worker)
        
        worker_resource = self.__cap / num_worker
        return [Worker(worker_resource) for _ in range(num_worker)]
    
    
    def get_available_worker(self) -> List[Worker]:
        """To execute any topology in this physical machine, we must need enough workers.

        Returns:
            List[Worker]: List of available workers
        """
        availale_worker = []
        for idx, worker in enumerate(self.__worker):
            if not worker.assigned:
                availale_worker.append(idx)
        return availale_worker
        
        
    def assign_topology(self, graph) -> bool:
        """Assign subgraph of the topology in this physical machine if there is available worker.

        Args:
            graph (_type_): _description_
        """
        
        if len(self.get_available_worker()) <= 0:
            return False
        
        selected = 0
        for idx, worker in enumerate(self.__worker):
            if not worker.assigned:
                selected = idx
        
        self.__worker[selected].assigned = True
        self.__worker[selected].assign(graph)
        return True