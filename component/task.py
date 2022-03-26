import time
import uuid

from numpy import Inf

class Task:
    CNT = 0
    """Communication in task is usually inter-threads in a process. The exception is only sending a global vertex(or task).
    """
    def __init__(self, model, iter=Inf, name=None):
        """_summary_

        Args:
            model (_type_): latency model
            iter (_type_): iteration how many times execute this task. The default is infinite.
        """
        if name is not None:
            #self._id = name + '-' + str(uuid.uuid1())
            self._id = name
        else:
            self._id = 'task-' + str(Task.CNT)
        Task.CNT += 1
        self._latency_generator = model
        self._iterations = iter
        self._throughput = 0
        self._input_rate = 0
        self._sum_interrupt_latency = 0.0
        
    def __str__(self):
        ret = f'  {self._id}: {self._latency_generator}, {self._iterations}'
        return ret
        
        
    @property
    def id(self):
        return self._id

    def _post_result(self):
        pass
        
    def start(self):
        for _ in range(self._iterations):
            latency = self._latency_generator.next_latency_ms()
            stime = time.time()
            time.sleep(latency)
            elapsed = time.time() - stime
            self._sum_interrupt_latency += elapsed
            self._throughput += 1
            
            self._post_result()