import uuid
import random as rd

class Vertex:
    TYPE = ['source', 'operator']
    def __init__(self, num_thread, cpu_load, memory_load, num_parallelism, type, latency_simulator):
        self.__id = 'vertex-' + str(uuid.uuid1())
        self.__required_thread = num_thread
        self.__cpu_load = cpu_load
        self.__memory_load = memory_load
        self.__type = type
        self.__parallelism = num_parallelism
        self._latency_simulator = latency_simulator
        
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
    
class RandomVertex(Vertex):
    def __init__(self, max_thread, max_cpu, max_memory, max_parallelism, type):
        self.__id = 'vertex-' + str(uuid.uuid1())
        self.__required_thread = rd.randint(1, max_thread)
        self.__cpu_load = rd.randint(1, max_cpu)
        self.__memory_load = rd.randint(1, max_memory)
        self.__type = type
        self.__parallelism = rd.randint(1, max_parallelism)
    
    