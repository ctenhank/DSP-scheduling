import time
from typing import List
import numpy
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.worker import Worker

class Simulator:
    def __init__(self, cluster: Cluster, model: str='normal'):
        """_summary_

        Args:
            cluster (Cluster): _description_
            model (str): Latency model. The general latency generator model has the normal distribution.
        """
        self._cluster = cluster
        self._jitter_model = self._select_jitter_model(model)
    
    def _select_jitter_model(self, model: str):
        if model == 'normal':
            return numpy.random.standard_normal        
    
    def start_benchmark(self):
        nodes = self._cluster.nodes
        executable: List[Worker] = []
        
        for node in nodes:
            for worker in node.worker:
                if worker.assigned:
                    executable.append(worker)
                    
        while True:
            for worker in executable:
                #print(f'{type(worker.graph)}: {worker.graph}')
                
                for task in worker.graph.tasks:
                    task.start()
                    #print(task)