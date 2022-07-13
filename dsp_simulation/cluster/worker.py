from dsp_simulation.topology.task_graph import SubTaskGraph

class Worker:
    """This class is a process to execute and handle a subtopology of a topology in physical machine.
    It can process only one subtopology per worker.
    """
    
    CNT = 0
    
    def __init__(self, speed_up, pn_id):
        self._id = 'worker-' + str(Worker.CNT)
        self._pn_id = pn_id
        self.assigned = False
        self._speed_up = speed_up
        
        self._graph: SubTaskGraph = None
        Worker.CNT += 1
            
    @property
    def speed_up(self):
        return self._speed_up
        
    @property
    def id(self):
       return self._id
   
    @property
    def pn_id(self):
        return self._pn_id
    
    @property
    def graph(self):
        return self._graph
        
    def assign(self, graph: SubTaskGraph):
       self.assigned = True
       self._graph = graph
    
    def deassign(self):
        self.assigned = False
        self._graph = None