from typing import List

from dsp_simulation.topology.task import Task
import uuid

class SubTopology:
    def __init__(self, task, topology_id):
        self.__id = 'subgraph-' + str(uuid.uuid1())
        self.__topology_id = topology_id
        self.__tasks: List[Task] = task
        
    def __str__(self):
        ret = f'{self.__id}(tasks): '
        for task in self.__tasks:
                ret += str(task.id) + ', '
        return ret[:-2]
        
    @property
    def tasks(self):
        return self.__tasks
    
    @property
    def id(self):
        return self.__id
    
    @property
    def topology_id(self):
        return self.__topology_id