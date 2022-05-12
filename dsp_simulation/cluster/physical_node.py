import random as rd

from typing import List
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.topology.task import OperatorTask
from dsp_simulation.topology.task_graph import SubTaskGraph


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
    AVAILABILTY = (97, 99.999)
    #CAPABILITY = [50.0, 100.0, 200.0, 400.0]
    #CAPABILITY = [50, 75, 100, 125, 150]
    MINIMAL_REQUIRED_RESOURCES_PER_WORKER = 50.0

    def __init__(self, max_worker, rack=None):
        self._id = 'node-' + str(PhysicalNode.CNT)
        PhysicalNode.CNT += 1
        #self._cap = rd.choice(PhysicalNode.CAPABILITY)
        self._num_core = rd.randint(4, 8)
        #self._remains = self._cap
        self._rack: List[str] = rack
        self._speed_up = float(int(rd.uniform(0.8, 1.3) * 10) / 10)
        self._worker: List[Worker] = self._get_workers(max_worker)
        self._available_worker: List[bool] = [True for _ in range(len(self._worker))]
        self._available_worker_cnt: int = len(self._worker)
        self._availabilty = rd.uniform(
            PhysicalNode.AVAILABILTY[0], PhysicalNode.AVAILABILTY[0])
    
       
    def __str__(self):
       ret = f'{self._id}, {self._speed_up}, {self._rack}, {len(self._worker)}'
       return ret

    @property
    def id(self):
       return self._id

    #@property
    #def cap(self):
    #   return self._cap

    #@property
    #def remains(self):
    #   return self._remains

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

    @available_worker_cnt.setter
    def available_worker_cnt(self, cnt):
       self._available_worker_cnt = cnt

    @property
    def availability(self):
       return self._availabilty
   
    @property
    def speed_up(self):
        return self._speed_up

    def _get_workers(self, max_worker: int):
        """generate and get workers based on max_worker or the amount of resources

        Args:
            max_worker (int): the maximum number of worker. It can be changed depending on the amount of resources.

        Returns:
            List[Worker]: List of created Workers
        """
        # Resource에 따라서 reconfigure
        #internal_max_worker = int(self._cap / PhysicalNode.MINIMAL_REQUIRED_RESOURCES_PER_WORKER)
        #if max_worker > internal_max_worker:
        #    max_worker = internal_max_worker

        #num_worker = rd.randint(1, max_worker)

        #worker_resource = self._cap / num_worker
        #return [Worker(worker_resource) for _ in range(num_worker)]
        #print(self._speed_up)
        return [Worker(self.speed_up, self._id) for _ in range(self._num_core)]

    def get_available_worker(self) -> List[Worker]:
        """To execute any topology in this physical machine, we must need enough workers.

        Returns:
            List[Worker]: List of available workers
        """
        ret = []
        for idx, available in enumerate(self._available_worker):
            if available:
                ret.append(self._worker[idx])

        return ret
    
    def assign(self, subgraph: SubTaskGraph):
        """Assign subgraph of the topology in this physical machine if there is available worker.

        Args:
            bool: 
        """        
            
        if len(self.get_available_worker()) <= 0:
            return None
        
        selected = -1
        for idx in range(len(self._worker)):
            if self._available_worker[idx]:
                selected = idx
                self._available_worker[selected] = False
                self._available_worker_cnt -= 1
                break
            
        self._worker[selected].assign(subgraph)
        for task in subgraph.task:
            if type(task) == OperatorTask:
                task.update_speed_up(self._worker[selected].speed_up)                
                
        return self._worker[selected]

    def deassign(self, worker_id: str):
        """Deassign subgraph in the given worker

        Args:
            idx (int): Index of the worker
        """
        for idx, worker in enumerate(self._worker):
            if worker.id == worker_id:
                self._available_worker[idx] = True
                self._available_worker_cnt += 1
                self._worker[idx].deassign()
                break
