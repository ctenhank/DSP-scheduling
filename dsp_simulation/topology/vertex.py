from abc import abstractmethod
from typing import List
from dsp_simulation.simulator.generator import Generator

import uuid

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
    def __init__(self, max_data_rate=10000, name=None):
        super().__init__(name)
        self._max_data_rate = max_data_rate
        self._data_size = 0
        self._input_dist = None
    
    @property
    def data_rate(self):
        return self._max_data_rate
    
    
    @property
    def input_dist(self):
        return self._input_dist
    
    def type(self):
        return SourceVertex.__class__
    
    def update_rate_distribution(self, dist):
        self._input_dist = dist


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
    PATTERN = ['aggregate', 'map', 'filter', 'flatmap']
    def __init__(self, parallelism=3, selectivity=5, productivity=0.2, name=None, min_parallelism=1, max_parallelism=200, latency_generator:Generator=None):
        super().__init__(name)
        self._parallelism = parallelism
        self._indegree: List[Vertex] = []
        self._selectivity = selectivity
        self._productivity = productivity
        self._min_parallelism = min_parallelism
        self._max_parallelism = max_parallelism
        self._latency_generator = latency_generator
  
    @property
    def parallelism(self):
        return self._parallelism
    
    @parallelism.setter
    def parallelism(self, parallelism):
        if parallelism < self._min_parallelism and parallelism > self._max_parallelism:
            print(f'The parallelism of vertex must be over and equal than {self._min_parallelism} and less than {self._max_parallelism}')
            return
        self._parallelism = parallelism
        
    @property
    def indegree(self):
        return self._indegree
    
    @property
    def selectivity(self):
        return self._selectivity
    
    @property
    def productivity(self):
        return self._productivity
    
    @property
    def latency_generator(self):
        return self._latency_generator
    
    def type(self):
        return OperatorVertex.__class__
    
    def add_indegree(self, target):
        self._indegree.append(target)
