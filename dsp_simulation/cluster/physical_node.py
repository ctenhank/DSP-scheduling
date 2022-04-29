from typing import List
import random as rd

from dsp_simulation.cluster.worker import Worker
from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator
#from dsp_simulation.topology.subtopology import SubTopology
from dsp_simulation.topology.topology import Topology

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
   CAPABILITY = [50, 75, 100, 125, 150]
   MINIMAL_REQUIRED_RESOURCES_PER_WORKER = 50.0
    
   def __init__(self, max_worker, rack=None):
       self.__id = 'node-' + str(PhysicalNode.CNT)
       PhysicalNode.CNT += 1
       self._cap = rd.choice(PhysicalNode.CAPABILITY)
       self._num_core = rd.randint(1, 8)
       self._remains = self._cap
       self._rack: List[str] = rack
       self._worker: List[Worker] = self._get_workers(max_worker)
       self._available_worker: List[bool] = [True for _ in range(len(self._worker))]
       self._available_worker_cnt: int = len(self._worker)
       self._availabilty = rd.uniform(PhysicalNode.AVAILABILTY[0], PhysicalNode.AVAILABILTY[0])


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
   
   @available_worker_cnt.setter
   def available_worker_cnt(self, cnt):
       self._available_worker_cnt = cnt
   
   @property
   def availability(self):
       return self._availabilty
    
    
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
       worker_resource = self._cap
       return [Worker(worker_resource) for _ in range(self._num_core)]
    
    
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
    
   def _get_latency_dist_from_topology(self, topology: Topology, cap: int, task_id:str):
        key = ''
        
        if 'count' in task_id:
            key = f'count-{cap}'
        elif 'split' in task_id:
            key = f'split-{cap}'
        #else:
        #    key = f'split-{cap}'
        #else:
        #    return None
    #   elif 'source' in task_id:
    #       key = f'spout-{cap}'
    #   elif 'sink' in task_id:
    #       key = f'report-{cap}'
       #key += '-500'
       
        
        #print(f'{task_id}: {topology.distribution[key]}')
        return topology.distribution[key]
    
   def assign(self, topology: Topology, subgraph_idx: int):
        """Assign subgraph of the topology in this physical machine if there is available worker.

        Args:
            bool: 
        """        
        ret = {
            'success': False,
            'assigned_location': None
        }
            
        if len(self.get_available_worker()) <= 0:
            return ret
        
        selected = -1
        for idx in range(len(self._worker)):
            if self._available_worker[idx]:
                selected = idx
                self._available_worker[selected] = False
                self._available_worker_cnt -= 1
                break
            
        self._worker[selected].assign(topology.taskgraph.subgraph[subgraph_idx])
            
        ret['success'] = True
        ret['assigned_location'] = (self.__id, self._worker[selected].id)
            
        for task in topology.taskgraph.subgraph[subgraph_idx].task:
            if topology.is_operator(task.vertex_id):
                    task.assigned_cap = self._worker[selected].capability
                    dist = self._get_latency_dist_from_topology(topology, task.assigned_cap, task.id)
                    task.update_latency_generator(GaussianLatencyGenerator(dist[0], dist[1]))
                
                
        print(f'Assigned {str(topology.taskgraph.subgraph[subgraph_idx].id)} on {self._worker[selected].id} of {self.__id}')
        return ret
    
   def deassign(self, idx):
       """Deassign subgraph in the given worker

       Args:
           idx (int): Index of the worker
       """
       self._available_worker[idx] = True
       self._available_worker_cnt += 1
       self._worker[idx].deassign()
       print(f'Deassigned {self._worker[idx].id} of {self.__id}')