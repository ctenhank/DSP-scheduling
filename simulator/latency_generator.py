from cmath import sqrt
import numpy as np
import time

class LatencyGenerator():
    """This class generates latency in the given latency_model, the default is gaussian noise model.
    """
    def __init__(self, latency_model, mean, std) -> None:
        """Late

        Args:
            latency_model (_type_): _description_
            mean (_type_): _description_
            std (_type_): _description_
        """
        self._latency_model = latency_model
        self._mean = mean
        self._std = std
        
    def _get_noise(self):
        return np.random.standard_normal()
    
    def next_latency_ms(self):
        return (self._get_noise() * self._std + self._mean) / 1000
    

class SomeAwesomeSimulator():
    def __init__(self, latency_generator) -> None:
        self._latency_generator = latency_generator
        self._iterations = 100
        self._sum_interrupt_latency = 0.0
        self._throughput = 0
        
    def _post_result(self):
        pass
    
    def start_benchmark(self):
        for i in range(10000):
            l = self._latency_generator.next_latency_ms()
            #print(l)
            stime = time.time()
            time.sleep(l)
            elapsed_time = time.time() - stime
            self._sum_interrupt_latency += elapsed_time
            self._throughput += 1
            
            
            #print(f'elapsed_time: {elapsed_time}')
            self._post_result()
        print(f"Average latency of sleep interrupts: {self._sum_interrupt_latency / self._count_interrupt}")