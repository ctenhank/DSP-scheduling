import uuid
class Worker:
    def __init__(self, capability):
        self.__id = 'worker-' + str(uuid.uuid1())        
        self.__cap = capability
        self.assigned = False
        self.__graph = [ ]
        
    @property
    def required_resource(self):
        return self.__cap
        
    #@property
    #def assigned(self):
    #    return self.__assigned
        
    def assign(self, graph):
        self.assigned = True
        self.__graph = graph
    
    
        