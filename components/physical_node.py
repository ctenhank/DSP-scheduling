import uuid

class PhysicalNode:
    FAILURE = 0.025
    def __init__(self, cpu_capability, memory_capability, rack, num_worker):
        self.__id = 'node-' + str(uuid.uuid1())
        self.__cap_cpu = cpu_capability
        self.__cap_memory = memory_capability
        self.__remaining_cpu= cpu_capability
        self.__remaining_memory = memory_capability
        self.__rack = rack
        self.__num_worker = num_worker
        
        self.__failure = PhysicalNode.FAILURE
        self.__assignment = []
        
    def __str__(self):
        ret = f'{self.__id}, {self.__cap_cpu}, {self.__cap_memory}, {self.__rack}, {self.__remaining_cpu}, {self.__remaining_memory}, {self.__num_worker}'
        return ret
    
    @property
    def id(self):
        return self.__id
    
    @property
    def cpu_capability(self):
        return self.__cap_cpu
    
    @property
    def memory_capability(self):
        return self.__cap_memory
    
    @property
    def rack(self):
        return self.__rack
    
    @property
    def num_worker(self):
        return self.__num_worker
    
    @property
    def remaining_cpu(self):
        return self.__remaining_cpu
    
    @property
    def remaining_memory(self):
        return self.__remaining_memory
    
        
    def __check_capability(self, requirements):
        if (self.__capability - self.__occupied_cap) >= requirements:
            return True
        return False
    
    