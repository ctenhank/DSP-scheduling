import uuid
class Worker:
    def __init__(self, capability):
        self.__id = 'worker-' + str(uuid.uuid1())        
        self.__cap = capability
        self.assigned = False
        self.__graph = []
        
    @property
    def capability(self):
        return self.__cap
    
    @property
    def id(self):
        return self.__id
    
    @property
    def graph(self):
        return self.__graph
        
    def assign(self, graph):
        self.assigned = True
        self.__graph = graph
    
    def deassign(self):
        self.assigned = False
        self.__graph = []