from typing import List
import uuid
from dsp_simulation.topology.subtopology import SubTopology

from dsp_simulation.topology.task import Task
class Worker:
    """This class is a process to execute and handle a subtopology of a topology in physical machine.
    It can process only one subtopology per worker.
    """
    def __init__(self, capability):
        self.__id = 'worker-' + str(uuid.uuid1())        
        self.__cap = capability
        self.assigned = False
        self.__graph: SubTopology = []
        
    @property
    def capability(self):
        return self.__cap
    
    @property
    def id(self):
        return self.__id
    
    @property
    def graph(self):
        return self.__graph
        
    def assign(self, graph: SubTopology):
        self.assigned = True
        self.__graph = graph
    
    def deassign(self):
        self.assigned = False
        self.__graph = []