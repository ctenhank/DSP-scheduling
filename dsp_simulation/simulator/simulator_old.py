from itertools import accumulate
import sys
from datetime import datetime
from typing import List
import numpy
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator
from dsp_simulation.topology.topology import Topology
import pickle as pkl
from pathlib import Path
import pandas as pd

class Simulator:
    def __init__(self, cluster: Cluster, topology: Topology, scheduler: Scheduler, outdir='./data/', type='wc',  tot_time=90*1000):
        """_summary_

        Args:
            cluster (Cluster): _description_
            model (str): Latency model. The general latency generator model has the normal distribution.
        """
        self._cluster = cluster
        self._topology = topology
        self._jitter_model = self._select_latency_distribution(type)
        self._scheduler= scheduler
        self._outpath = Path(outdir) / self._scheduler.__class__.__name__
        self._simulation_time = tot_time
        self._distribution = None
    
    def _select_latency_distribution(self, type: str):
        if type == 'wc':
            self._distribution = pd.read_csv('./conf/wc_latency_model.csv').to_dict()
            
    def _min_tuple(self, tuples):
        pass
    
    def _finish(self):
        pass
    
    def start_benchmark(self):
        # TODO: Latency Distribution 설정 및 실행
        print(f'Start {self._scheduler.__class__.__name__} benchmark')
        stime = datetime.now()
        self._scheduler.schedule(self._cluster, self._topology)
        elasped_time = str(datetime.now() - stime)
        nodes = self._cluster.nodes
        executable: List[Worker] = []
        
        elapsed_time_for_worker = {}
        metrics = {
            'elapsed_time': elasped_time,
            'scheduler': self._scheduler.__class__.__name__
        }
        
        # initialization
        measured_time_ms = 0.0
        
        for node in nodes:
            for worker in node.worker:
                if worker.assigned:
                    
                    executable.append(worker)
                    metrics[worker.id] = {
                        'latency_for_worker': [],
                        'throughput': 0
                    }
                    elapsed_time_for_worker[worker.id] = {}
                    for task in worker.graph.tasks:
                        metrics[worker.id][task.id] = []
                        elapsed_time_for_worker[worker.id][task.id] = 0.0
        
        
        
        print(f'Start Tasks of {self._scheduler.__class__.__name__}')
        
        #elapsed_time_for_worker = {}
        #for worker in executable:
        #    elapsed_time_for_worker[worker.id] = {
        #        'elapsed_time': 0.0
        #    }
        #    for task in worker.graph.tasks:
        #        elapsed_time_for_worker[worker.id][task.id] = 0.0
        
        
        thread_distribution = GaussianLatencyGenerator(0.004380266651921919, 0.0006388322379872011)
        
        interval = 0
        while measured_time_ms < self._simulation_time:
            # Communication Modeling
            
            q, r = divmod(int(measured_time_ms), 5000)
            if q != interval and r == 0:
                interval = q
                print(f'Measured time: {measured_time_ms}')
                print(f"Now, {measured_time_ms/1000} (seconds)")
            
            
            
            for worker in executable:               
                #if elapsed_time_for_worker[worker.id]['elapsed_time'] < self._simulation_time:
                transmission_delay = 0.0                    
                accumulated_delay = 0.0
                # 어떤 태스크는 다른 PN에 배치된 서브 그래프의 태스크와 통신할 수 있음
                prev_task_order = 0
                prev_latencies = []
                for task in worker.graph.tasks:
                    if elapsed_time_for_worker[worker.id][task.id] >= self._simulation_time:
                        continue
                    
                    # 여기 해결해야함
                    if prev_task_order != task.order:
                        accumulated_delay += transmission_delay
                        transmission_delay = thread_distribution.next_latency_ms()
                        prev_task_order = task.order
                            
                    latency = task.start() + accumulated_delay
                    prev_latencies.append(latency)
                    elapsed_time_for_worker[worker.id][task.id] += latency
                    #print(f'{task.id}: {latency}')
                    metrics[worker.id][task.id].append(latency)
                
                print(elapsed_time_for_worker[worker.id])
                elapsed_time_for_worker[worker.id]['min_time'] = min(elapsed_time_for_worker[worker.id])
                #metrics[worker.id]['throughput'] += 1
    
                
            tuples = [(key, elapsed_time_for_worker[key]['elapsed_time']) for key in elapsed_time_for_worker]
            measured_time_ms = min(tuples, key=lambda t:t[1])[1]
            
            #for worker in executable:
            #    tot_latency = 0.0
                
                
            #    if elapsed_time_for_worker[worker.id]['elapsed_time'] < self._simulation_time:
            #        for task. in worker.graph.tasks:
            #            if elapsed_time_for_worker[worker.id][task.id] >= self._simulation_time:
            #                continue
                        
            #            latency = task.start()
            #            elapsed_time_for_worker[worker.id][task.id] += latency
            #            #print(f'{task.id}: {latency}')
            #            tot_latency += latency
            #            metrics[worker.id][task.id].append(latency)
            #        metrics[worker.id]['latency_for_worker'].append(tot_latency)
            #        metrics[worker.id]['throughput'] += 1
                    
            #        elapsed_time_for_worker[worker.id]['elapsed_time'] += tot_latency
    
                
            #tuples = [(key, elapsed_time_for_worker[key]['elapsed_time']) for key in elapsed_time_for_worker]
            #measured_time_ms = min(tuples, key=lambda t:t[1])[1]
        
        print(f'Finish Tasks of {self._scheduler.__class__.__name__}')
        
        with open(self._outpath, 'wb') as f:
            pkl.dump(metrics, f)
        print(f'Finish {self._scheduler.__class__.__name__} benchmark')
