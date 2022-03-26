import time
from numpy import Inf
import uuid

from dsp_simulation.simulator.latency_generator import LatencyGenerator

class Task:
    """Communication in task is usually inter-threads in a process. The exception is only sending a global vertex(or task).
    """
    def __init__(self, required_cap, model=None, name=None):
        """_summary_

        Args:
            model (_type_): latency model
            iter (_type_): iteration how many times execute this task. The default is infinite.
        """
        if name is not None:
            self._id = name
        else:
            self._id = 'task-' + str(uuid.uuid1())
        
        self._latency_generator: LatencyGenerator = model
        self._throughput = 0
        self._input_rate = 0
        self._required_cap = required_cap
        self._assigned_cap = 0.0
        self._sum_latency = 0.0
        self._measure_time = 0.0
        self._interval_time = 0
        
    def __str__(self):
        ret = f'  {self._id}: {self._latency_generator}'
        return ret
        
    @property
    def id(self):
        return self._id
    
    @property
    def latency_generator(self):
        return self._latency_generator
    
    @property
    def assigned_cap(self):
        return self._assigned_cap
    
    @assigned_cap.setter
    def assigned_cap(self, cap):
        self._assigned_cap = cap
    
    @latency_generator.setter
    def latency_generator(self, model):
        """_summary_

        Args:
            model (_type_): _description_
        """
        self._latency_generator = model
        
    def update_latency_generator(self):
        """Topology 및 Vertex에 따라서 요구되는 capability는 다르다. 또한 Worker에 어떻게 할당되느냐에 따라서 보여주는 성능 또한 다르다.
        이에 대한 반영이 필요하다.
        """
        if self._required_cap / self._assigned_cap >= 0:
            mean, std = 0.7190926125335194, 0.1
        else:
            mean, std = 0.7190926125335194, 0.1

        self._latency_generator.mean = mean
        self._latency_generator.std = std

    def _post_result(self, result):
        self._throughput += 1
        self._measure_time += result['execute_latency']
        #print(self._measure_time)
        q, r = divmod(int(self._measure_time), 2)
        if q != self._interval_time and r == 0:
            self._interval_time = q
            print(f'{self._id} ({self._measure_time}): throughput({self._throughput}), avg_latency({self._measure_time / self._throughput})')
        #print(f'{self._id}: result({result})')
        
    def start(self):
        """_summary_
        """
        stime = time.time()
        
        latency = self._latency_generator.next_latency_ms()
        time.sleep(latency)
        #execute_latency = (time.time() - stime) / 1000
        execute_latency = (time.time() - stime) 
        self._post_result({
            'process_late': latency,
            'execute_latency': execute_latency
        })