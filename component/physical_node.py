import enum
from typing import List
import uuid
import random as rd

from component.worker import Worker

class PhysicalNode:
    CNT = 0
    FAILURE = 0.025
    CAPABILITY = [50.0, 100.0, 200.0, 400.0]
    def __init__(self, rack, max_worker):
        #self.__id = 'node-' + str(uuid.uuid1())
        self.__id = 'node-' + str(PhysicalNode.CNT)
        PhysicalNode.CNT += 1
        self.__cap = rd.choice(PhysicalNode.CAPABILITY)
        self.__remains = self.__cap
        self.__rack = rack
        self.__num_worker = rd.randint(1, max_worker)
        self.__worker = self._get_workers()
        self.__failure = PhysicalNode.FAILURE

    def __str__(self):
        ret = f'{self.__id}, {self.__cap}, {self.__rack}, {self.__remains}'
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
    
    @property
    def worker(self):
        return self.__worker
    
    
    def _get_workers(self):
        worker_resource = self.__cap / self.__num_worker
        return [Worker(worker_resource) for _ in range(self.__num_worker)]
    
    
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
        
        
    def assign_topology(self, graph) -> None:
        """Assign subgraph of the topology in this physical machine if there is available worker.

        Args:
            graph (_type_): _description_
        """
        workers = self.get_available_worker()
        
        if len(workers) <= 0:
            print('There is no enough worker in this physical machine. Please increase the number of worker or place it to other machines.\n')
            exit(1)
            
        selected = rd.choice(range(len(workers)))
        workers[selected].assigned = True
        workers[selected].assign(graph)      
        
    
    