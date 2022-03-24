from ast import operator
import uuid
import random as rd

from components.vertex import *

class Vertex:
    TYPE = ['source', 'operator']
    
    def __init__(self, max_thread, max_cpu, max_memory, max_parallelism, type):
        self.__id = 'vertex-' + str(uuid.uuid1())
        self.__required_thread = rd.randint(1, max_thread)
        self.__cpu_load = rd.randint(1, max_cpu)
        self.__memory_load = rd.randint(1, max_memory)
        self.__type = type
        self.__parallelism = rd.randint(1, max_parallelism)
        
    def __str__(self):
        ret = f'{self.__id}, {self.__cpu_load}, {self.__memory_load}, {self.__type}'
        return ret
    
    @property
    def id(self):
        return self.__id
    
    @property
    def thread(self):
        return self.__required_thread
    
    @property
    def cpu_load(self):
        return self.__cpu_load
    
    @property
    def memory_load(self):
        return self.__memory_load
    
    @property
    def type(self):
        return self.__type
    

class Topology:
    RANDOM_KEY = ['max_thread', 'max_cpu', 'max_memory']
    PREDEFINED_TOPOLOGY = ['word_count']
    
    def __init__(self, random: bool, **kwargs):
        self.__id  = 'topology-' + str(uuid.uuid1())
        self.__source_vertices = []
        self.__operator_vertices = []
        self.__edge_src2op = []
        self.__edge_op2op = []
        
        if 'predefined_topology' in kwargs.keys():
            topololgy_name = kwargs.get('predefined_topology')
            if topololgy_name not in Topology.PREDEFINED_TOPOLOGY:
                print(f'No such topology name: {topololgy_name}. The supporting topologies is like {Topology.PREDEFINED_TOPOLOGY}')
                exit(1)
            
        
        if random:
            max_thread = 5
            max_cpu = 1000.0
            max_memory = 10000.0
            
            for key, value in kwargs.items():
                if key in Topology.RANDOM_KEY:
                    if key == Topology.RANDOM_KEY[0]:
                        max_thread = value
                    elif key == Topology.RANDOM_KEY[1]:
                        max_cpu = value
                    elif key == Topology.RANDOM_KEY[2]:
                        max_memory = value
                
            self._generate_random_topology(
                max_thread=max_thread,
                max_cpu=max_cpu,
                max_memory=max_memory
                )
            
    @property
    def id(self):
        return self.__id
            
    def __str__(self):
        ret = '=' * 25 + 'Topology Info' + '='* 25 + '\n'
        ret += 'Vertex Info: id, cpu, memory, type\n'
        for vertex in self.__source_vertices:
            ret += str(vertex) + '\n'
        for vertex in self.__operator_vertices:
            ret += str(vertex) + '\n'
        ret += f'Edge from source to operator:\n {self.__edge_src2op}\n'
        ret += f'Edge from operator to operator:\n {self.__edge_src2op}\n'
        ret += '=' * 50 + '\n'
        return ret
            
    def _generate_random_topology(self, max_thread, max_cpu, max_memory):
        available_thread = rd.randint(1, max_thread)
        available_cpu = rd.randint(1, int(max_cpu))
        available_memory = rd.randint(1, int(max_memory))
        
        idx = 0
        num_source = 0
        num_operator = 0
        while available_cpu > 0 and available_memory > 0 and available_thread > 0:
            vertex = None
            if idx % 2 == 0:
                vertex = RandomVertex(available_thread, available_cpu, available_memory, RandomVertex.TYPE[0])
                self.__source_vertices.append(vertex)
                num_source += 1
            else:
                vertex = RandomVertex(available_thread, available_cpu, available_memory, RandomVertex.TYPE[1])
                self.__operator_vertices.append(vertex)
                num_operator += 1
                
            available_thread -= vertex.thread
            available_cpu -= vertex.cpu_load
            available_memory -= vertex.memory_load
            idx += 1
            
        if num_operator > 0:
            for i in range(len(self.__source_vertices)):
                j = rd.randint(0, len(self.__operator_vertices))
                self.__edge_src2op.append((i, j))
        
        if num_operator > 1:
            for i in range(len(self.__operator_vertices)):
                j = rd.randint(0, len(self.__operator_vertices))
                while i != j:
                    j = rd.randint(0, len(self.__operator_vertices))
                self.__edge_op2op.append((i, j))
    
    def __generate_wordcount_topology(self):
        source = Vertex(1, 1, 1, Vertex.TYPE[0])
        split = Vertex(1, 1, 1, Vertex.TYPE[1])
        count = Vertex(1, 1, 1, Vertex.TYPE[2])
        
        self.__source_vertices.append(source)
        self.__operator_vertices.append(split)
        self.__operator_vertices.append(count)
        
        self.__edge_src2op.append((0, 0))
        self.__edge_op2op.append((0, 1))
        
        