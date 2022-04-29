from datetime import datetime
from typing import Dict, List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.network import Network
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.etc.clock import SystemClock
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator
from dsp_simulation.topology.task import OperatorTask, SinkTask, Task
from dsp_simulation.topology.topology import Topology
import pickle as pkl
from pathlib import Path
import pandas as pd
import random as rd
import dsp_simulation.topology.task as t

class TaskInfo:
    def __init__(self, worker: List[Worker]):
        self._worker = worker
        self.map_task_2_worker: Dict[Task, Worker] = {}
        self.worker_latency: Dict[Worker, float] = {}
        
        self.initialize()
    
        
    def initialize(self):
        for worker in self._worker:
            self.worker_latency[worker] = 0.0
            for task in worker.graph.task:
                self.map_task_2_worker[task] = worker
                
        

class Simulator:
    def __init__(self, cluster: Cluster, topology: Topology, scheduler: Scheduler, outdir='./data/', type='wc',  tot_time=900):
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
            
    def _get_source_worker(self, workers: List[Worker]) -> List[Worker]:
        ret = []
        rest = []
        for worker in workers:
            graph = worker.graph
            for task in graph.task:
                if self._topology.is_source(task.vertex_id):
                    ret.append(worker)
                else:
                    rest.append(worker)
        return ret, rest
    
    def check_strange(self):
        prev = SystemClock.CLOCK[0]
        cnt = 0
        for clk in SystemClock.CLOCK[1:]:
            if clk != prev + 1:
                cnt += 1
            prev = clk
        print(len(SystemClock.CLOCK), cnt)  
                
    def _order_operator_task(self, workers: List[Worker]):
        graph = self._topology.graph
        tier = 0
        order = {tier: ['root']}
        #tier += 1
        
        q = ['root']
        visited = ['root']
        while q:
            now = q.pop(0)
            #print(now)
            #print(ret[tier])
            if now in order[tier]:
                tier += 1
                
            for next in graph[now]:
                if next not in visited:
                    if tier not in order:
                        order[tier] = []
                    
                    order[tier].append(next)
                    q.append(next)
                visited.append(next)
        
        reorder = {}
        for key in order:
            if key == 0:
                continue
            
            vertices_id = order[key]
            for vertex_id in vertices_id:
                if self._topology.is_operator(vertex_id):
                    if key not in reorder:
                        reorder[key] = []
                    reorder[key].append(vertex_id)
                    
        ret = {}
        worker = {}
        tier = 0
        for key in reorder:
            if tier not in ret:
                ret[tier] = []
            
            for vertex in reorder[key]:
                for worker in workers:
                    graph = worker.graph
                    for task in graph.task:
                        if task.vertex_id == vertex:
                            ret[tier].append(task)
            tier += 1
            
        return ret
    
    
    def _initialize(self):
        nodes = self._cluster.nodes
        executable: List[Worker] = []
        for node in nodes:
            for worker in node.worker:
                if worker.assigned:
                    executable.append(worker)
                    #metrics[worker.id] = {
                    #    'latency_for_worker': [],
                    #    'throughput': 0
                    #}
                    #elapsed_time_for_worker[worker.id] = {}
                    #for task in worker.graph.task:
                    #    metrics[worker.id][task.id] = []
                    #    elapsed_time_for_worker[worker.id][task.id] = 0.0
    
    def _get_sink_task(self):
        ret = []
        for key in self._topology.taskgraph._task:
            if self._topology.is_sink(key):
                ret.extend(self._topology.taskgraph._task[key])
        return ret
    
    
    def start_benchmark(self):
        # TODO: Latency Distribution 설정 및 실행
        print(f'Start {self._scheduler.__class__.__name__} benchmark')
        stime = datetime.now()
        self._scheduler.schedule(self._cluster, self._topology)
        elasped_time = str(datetime.now() - stime)
        nodes = self._cluster.nodes
        executable: List[Worker] = []
        worker_to_node: Dict[Worker, PhysicalNode] = {}
        task_to_worker: Dict[Task, Worker] = {}
        sink_task:List[SinkTask] = self._get_sink_task()
        
        #tz = datetime.timezone(datetime.timedelta(hours=9))
        #now = datetime.datetime.now(tz)
        if 'Scheduler' in t.outdir.stem:
            t.outdir = t.outdir.parent
        t.outdir = t.outdir / self._scheduler.__class__.__name__
        
        elapsed_time_for_worker = {}
        metrics = {
            'elapsed_time': elasped_time,
            'scheduler': self._scheduler.__class__.__name__,
            'cluster_size': len(self._cluster.nodes),
            'subgraph_size': len(self._topology.taskgraph.subgraph)
        }
        
        # initialization
        for node in nodes:
            for worker in node.worker:
                if worker.assigned:
                    executable.append(worker)
                    worker_to_node[worker] = node
                    #metrics[worker.id] = {
                    #    'latency_for_worker': [],
                    #    'throughput': 0
                    #}
                    #elapsed_time_for_worker[worker.id] = {}
                    for task in worker.graph.task:
                        task_to_worker[task] = worker
                        #metrics[worker.id][task.id] = []
                        #elapsed_time_for_worker[worker.id][task.id] = 0.0
        
        print(f'Start Tasks of {self._scheduler.__class__.__name__}')
        
        source_worker, rest_worker = self._get_source_worker(executable)
        ordered_task = self._order_operator_task(rest_worker)
        
        interval = 0
        task_info = TaskInfo(executable)
        while SystemClock.CURRENT < self._simulation_time:
            # Communication Modeling
            SystemClock.CURRENT += 0.0001
            q, r = divmod(int(SystemClock.CURRENT), 5)
            if q != interval and r == 0:
                interval = q
                #print(measured_time_ms)
                #print(f'Measured time: {measured_time_ms}')
                print(f"Now, {SystemClock.CURRENT} (seconds)")
                
            for worker in source_worker:
                for task in worker.graph.task:
                    
                    res = task.start()
                    if res != None:
                        for msg in res['msg']:
                            edge: List[Task] = worker.graph.edge[task]['target']
                            target = rd.choice(edge)
                            
                            #print(task_to_worker, target)
                            target_worker = task_to_worker[target]
                            source_node = worker_to_node[worker]
                            target_node = worker_to_node[target_worker]
                            
                            type = Network.TYPE[0]
                            if source_node.id != target_node.id:
                                if source_node.rack != target_node.rack:
                                    type = Network.TYPE[3]
                                else:
                                    type = Network.TYPE[2]
                            
                            if worker.id != target_worker.id:
                                type = Network.TYPE[1]
                            
                            transmission_delay = Network.DISTRIBUTION[type].next_latency_ms()
                            #print(f'{task} to {target}: {delay}')
                            
                            msg.update_transmission_delay(transmission_delay)
                            msg.update_accumulated_latency(transmission_delay)
                            target.receive(task.vertex_id, msg)
                
            for key in ordered_task:
                for task in ordered_task[key]:
                    
                    #if self._topology.is_sink(task.vertex_id):
                    #    print(task.id)
                    #    task.start()
                    #else:
                    res = task.start()
                    if res != None:
                        for msg in res['msg']:
                            edge = task_to_worker[task].graph.edge[task]['target']
                            target = rd.choice(edge)
                                
                            target_worker = task_to_worker[target]
                            source_node = worker_to_node[worker]
                            target_node = worker_to_node[target_worker]
                            
                            type = Network.TYPE[0]
                            if source_node.id != target_node.id:
                                if source_node.rack != target_node.rack:
                                    type = Network.TYPE[3]
                                else:
                                    type = Network.TYPE[2]
                            
                            if worker.id != target_worker.id:
                                type = Network.TYPE[1]
                    
                            #delay = Network.SCALE[type] * Network.DISTRIBUTION.next_latency_ms()
                            transmission_delay = Network.DISTRIBUTION[type].next_latency_ms()
                            #print(f'{task} to {target}: {delay}')
                            msg.update_transmission_delay(transmission_delay)
                            msg.update_accumulated_latency(transmission_delay)
                            #msg.update_accumulated_latency(delay)
                            #print(target)
                            target.receive(task.vertex_id, msg)                
                            #print(f'Send a message from {task} to {target}')
                            
                for task in sink_task:
                    task.start()
        for vertex in self._topology.taskgraph._task:
            tasks: List[Task] = self._topology.taskgraph._task[vertex]
            for task in tasks:
                task.shutdown()
        print(f'Finish Tasks of {self._scheduler.__class__.__name__}')
        
        with open(self._outpath, 'wb') as f:
            pkl.dump(metrics, f)
        print(f'Finish {self._scheduler.__class__.__name__} benchmark')
        
        SystemClock.CURRENT = 0