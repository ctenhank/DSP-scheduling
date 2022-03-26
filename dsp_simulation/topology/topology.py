from collections import deque
from typing import List, Tuple
import uuid
import random as rd
from dsp_simulation.topology.subtopology import SubTopology
from dsp_simulation.topology.task import Task
from dsp_simulation.topology.vertex import Vertex
    
class Topology:
    """Acyclic graph consisting of logical operators
    TODO:
        1. Topology가 Subgraph로 분리됐지만 다른 Worker에 할당된 Subgrpah의 Task와 통신해야 하는 경우
    
    """
    def __init__(self, name, vertex:List[Vertex], edge:List[Tuple[Task, Task]]):
        if name == None:
            self.__id  = 'topology-' + str(uuid.uuid1())
        else:
            self.__id = name
            
        self.__vertex = vertex
        self.__edge = edge
        self.__subgraph: List[SubTopology] = self.__partitioning()
            
    @property
    def id(self):
        return self.__id
    
    @property
    def subgraph(self):
        return self.__subgraph
    
    def __str__(self):
        ret = '=' * 25 + 'Topology Info' + '='* 25 + '\n'
        ret += 'Vertex Info: id, capability, type, parallelism\n'
        for vertex in self.__vertex:
            ret += str(vertex) + '\n'
        ret += f'Edge from vertex to vertex:\n {self.__edge}\n'
        ret += '=' * 50 + '\n'
        return ret
            
    def __partitioning(self):
        ret = []
        tasks = []
        visited = {}
        
        for vertex in self.__vertex:
            tasks.extend(vertex.tasks)
    
        for task in tasks:
            visited[task.id] = False
        
        # BFS
        queue = deque()
        for task in tasks:
            if visited[task.id] is False:
                queue.append(task)
                subgraph_tasks = [task]
                while queue:
                    task_ = queue.popleft()
                    if visited[task_.id]:
                        continue
                    
                    visited[task_.id] = True
                                        
                    for edge in self.__edge:
                        if task_ in edge:
                            task1, task2 = edge[0], edge[1]
                            if task_.id != task1.id and not visited[task1.id]:
                                queue.append(task1)
                                subgraph_tasks.append(task1)
                            if task_.id != task2.id and not visited[task2.id]:
                                queue.append(task2)
                                subgraph_tasks.append(task2)
                        
                ret.append(SubTopology(list(set(subgraph_tasks)), self.__id))
        return ret