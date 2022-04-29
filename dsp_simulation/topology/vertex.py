from abc import abstractmethod
from asyncio import Task
import uuid
from typing import List

class Vertex:       
    def __init__(self, name=None):
        if name is None:
            self._id = 'vertex-' + str(uuid.uuid1())
        else:
            self._id = name
      
        self._outdegree: List[Vertex] = []
        self._parallelism = 1

    def __str__(self):
        return f'Vertex: {self._id}, {self.type}, {self._parallelism}'
    
    @property
    @abstractmethod
    def type(self):
        pass        

    @property
    def id(self):
        return self._id

    @property
    def outdegree(self):
        return self._outdegree
    
    def add_outdegree(self, target):
        self._outdegree.append(target)

    
class SourceVertex(Vertex):
    """SourceVertex has conceptually no indegree.

    Args:
        Vertex (_type_): _description_
    """
    def __init__(self, data_rate=10000, name=None):
        super().__init__(name)
        self._data_rate = data_rate
        self._data_size = 0
    
    @property
    def data_rate(self):
        return self._data_rate
    
    def type(self):
        return SourceVertex.__class__


class SinkVertex(Vertex):
    """SourceVertex has conceptually no outdegree.

    Args:
        Vertex (_type_): _description_
    """
    def __init__(self, name=None):
        super().__init__(name)
        self._indegree: List[Vertex] = []
    
    @property
    def indegree(self):
        return self._indegree
    
    def type(self):
        return SinkVertex.__class__
    
    def add_indegree(self, target):
        self._indegree.append(target)

    
class OperatorVertex(Vertex):
    """If there are over 2 degrees, this OperatorVertex should be blocked up to coming from every its source.
    
    """        
    def __init__(self, parallelism=3, selectivity=5, productivity=0.2, name=None):
        super().__init__(name)
        self._parallelism = parallelism
        self._indegree: List[Vertex] = []
        self._selectivity = selectivity
        self._productivity = productivity
  
    
    @property
    def parallelism(self):
        return self._parallelism
        
    @property
    def indegree(self):
        return self._indegree
    
    @property
    def selectivity(self):
        return self._selectivity
    
    @property
    def productivity(self):
        return self._productivity
    
    def type(self):
        return OperatorVertex.__class__
    
    def add_indegree(self, target):
        self._indegree.append(target)
