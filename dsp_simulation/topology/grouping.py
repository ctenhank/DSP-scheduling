from abc import *
from typing import List
from dsp_simulation.topology.task import Task
from dsp_simulation.topology.vertex import Vertex
import random as rd

class Grouping(metaclass=ABCMeta):
    def __init__(self, source:List[Task], target: List[Task]):
        self._source = source
        self._target = target
        self._visited_source = [False for _ in range(len(source))]
        self._visited_target = [False for _ in range(len(target))]
        self._transfer_rate = 1
        
    @abstractmethod
    def connect(self):
        pass
    
class GlobalGrouping(Grouping):
    def __init__(self, source:List[Task], target: List[Task]):
        super().__init__(source, target)
        
    #def connect(self):
    #    edges = []
    #    for task in self._source:
    #        edges.append(((task.id, self._target[0].id), self._transfer_rate))
    #    return edges
    
    def connect(self):
        edges = []
        for task in self._source:
            edges.append(((task, self._target[0]), self._transfer_rate))
        return edges

class AllGrouping(Grouping):
    def __init__(self, source: List[Task], target: List[Task]):
        super().__init__(source, target)

    #def connect(self):
    #    edges = []
    #    for task1 in self._source:
    #        for task2 in self._target:
    #            edges.append(((task1.id, task2.id), self._transfer_rate))
    #    return edges
    
    def connect(self):
        edges = []
        for task1 in self._source:
            for task2 in self._target:
                edges.append(((task1, task2), self._transfer_rate))
        return edges

class ShuffleGrouping(Grouping):
    """Distribute uniformly data of the source operator to the target operator.

    Args:
        Grouping (_type_): _description_
    """
    def __init__(self, source:List[Task], target: List[Task]):
        super().__init__(source, target)
        self._transfer_rate = 1 / len(target)
        
    #def connect(self):
    #    edges = []
    #    for task1 in self._source:
    #        for task2 in self._target:
    #            edges.append(((task1.id, task2.id), self._transfer_rate))
    #    return edges
    
    def connect(self):
        edges = []
        for task1 in self._source:
            for task2 in self._target:
                edges.append(((task1, task2), self._transfer_rate))
        return edges

        # if len(self._source.tasks) <= len(self._target.tasks):
        #     for idx, task in enumerate(self._source.tasks):
        #         if self._visited_source[idx]:
        #             continue
                
        #         self._visited_source[idx] = True
        #         target_idx = rd.randint(0,len(self._target.tasks)-1)
        #         while self._visited_target[target_idx]:
        #             target_idx = rd.randint(0,len(self._target.tasks)-1)
        #         self._visited_target[target_idx] = True
        #         edges.append((self._source.tasks[idx], self._target.tasks[target_idx]))    
            
                
        #     for idx, task in enumerate(self._target.tasks):
        #         if self._visited_target[idx]:
        #             continue
        #         self._visited_target[idx] = True
        #         source_idx = rd.randint(0,len(self._source.tasks)-1)
        #         edges.append((self._source.tasks[source_idx], self._target.tasks[idx]))   
                
        # else:
        #     for idx, task in enumerate(self._target.tasks):
        #         if self._visited_target[idx]:
        #             continue
                
        #         self._visited_target[idx] = True
        #         source_idx = rd.randint(0, len(self._source.tasks) -1)
        #         while self._visited_source[source_idx]:
        #             source_idx = rd.randint(0, len(self._source.tasks) -1)
        #         self._visited_source[source_idx] = True
        #         edges.append((self._source.tasks[source_idx], self._target.tasks[idx]))    
                
        #     for idx, task in enumerate(self._source.tasks):
        #         if self._visited_source[idx]:
        #             continue
                
        #         target_idx = rd.randint(0, len(self._target.tasks)-1)
        #         edges.append((self._source.tasks[idx], self._target.tasks[target_idx]))
                            
        #     #for idx, source_task in enumerate(source.tasks):
        #     #    if visited_source[idx]:
        #     #        continue
                
        #     #    visited_source[idx] = True
        #     #    target_idx = rd.randint(0,len(target.tasks)-1)
        #     #    edges.append(source_task, target.tasks[id])
        # return edges
