import random as rd
from typing import List
from components.vertex import Vertex
"""This file is implementation related to the grouping function which connect between two vertices in the topology.
There are three cases in grouping as following:
  - len(prev) == len(next): 1:1 matching
  - len(prev) > len(next): 
  - len(prev) < len(next):  
"""

class Grouping:
    def __int__(self, prev, next):
        self._prev = prev
        self._next = next
    
    def _determine_cases(self, prev, next):
        ret = 0
        if len(prev) > len(next):
            ret = 1
        return ret
    
    def shuffle_grouping(self):
        if len(self._prev) > len(self._next):
            pass
        else:
            pass
    
    def global_grouping(self):
        pass

def shuffle_grouping(from_: List[Vertex], to: List[Vertex]):
    """
    There must be an edge for each 'to' whatever the number of from_ and to.
    
    Randomly make a edge between from and to.
    In the case, the number of from is more larger than to, 
    
    """
    edges = []
    from_remains = []
    idx = 0
    for node in to:
        fr = rd.choice(from_)
        edges.append((node.id, fr.id))
        idx += 1
            
    if len(from_) > len(to):
        
        pass
    else:
        connect = []
        for prev in from_:
            pass
    
def global_grouping(from_: List[Vertex], to: List[Vertex]):
    #if len(to) > 0:
        
    edges = []
    for f in from_:
        edges.append((f.id, to[0]))
    pass
        
        
#def shuffle_grouping(prev, next):
    