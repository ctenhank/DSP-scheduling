from datetime import datetime
from typing import Dict, List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.network import Network
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.etc.clock import SystemClock
from dsp_simulation.runtime.profiler import Profiler
from dsp_simulation.scheduler.objective import Objective
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.task import OperatorTask, SinkTask, Task
from dsp_simulation.topology.topology import Topology
import pickle as pkl
from pathlib import Path
import pandas as pd
import random as rd
import dsp_simulation.topology.task as t                
        

class Simulator:
    def __init__(self, cluster: Cluster, topology: Topology, scheduler: Scheduler, profiler: Profiler, outdir='./data/', type='wc',  tot_time=900, time_freq=10000, period=1):
        """_summary_

        Args:
            cluster (Cluster): _description_
            model (str): Latency model. The general latency generator model has the normal distribution.
        """
        self._cluster = cluster
        self._topology = topology
        self._jitter_model = self._select_latency_distribution(type)
        self._scheduler= scheduler
        self._profiler = profiler
        self._outpath = Path(outdir) / self._scheduler.__class__.__name__
        self._simulation_time = tot_time
        self._distribution = None
        self._freq = 1 / time_freq
        self._network = Network()
        self._period = period
        
    
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
                    break
                else:
                    rest.append(worker)
                    break
        return ret, rest  
                
    def _order_operator_task(self, workers: List[Worker]):
        graph = self._topology.graph
        tier = 0
        order = {tier: ['root']}
        
        q = ['root']
        visited = ['root']
        while q:
            now = q.pop(0)
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
    
    def _get_sink_task(self):
        ret = []
        for key in self._topology.taskgraph._task:
            if self._topology.is_sink(key):
                ret.extend(self._topology.taskgraph._task[key])
        return ret
    
    def _get_maximum_input_rate(self):
        ret = 0
        for topology in self._cluster.topology:
            for source in topology.source:
                ret = max(ret, source.data_rate)
        return ret
    
    def _start_scheduling(self):
        stime = datetime.now()
        assignment = self._scheduler.schedule(self._cluster, self._topology)
        self._cluster.assign_topology(self._topology, assignment)
        elasped_time = str(datetime.now() - stime)
        
        if 'Scheduler' in t.outdir.stem:
            t.outdir = t.outdir.parent
        t.outdir = t.outdir / self._scheduler.__class__.__name__
        
        scheduler_log = {
            0:{
                'elapsed_time': elasped_time,
                'scheduler': self._scheduler.__class__.__name__,
                'cluster_size': len(self._cluster.nodes),
                'subgraph_size': len(self._topology.taskgraph.subgraph),
                'fitness': Objective.objectvie_weighted_sum(assignment=assignment)
                }
        }
        
        with open(self._outpath, 'wb') as f:
            pkl.dump(scheduler_log, f)
            
    def _shutdown_task(self):
        for vertex in self._topology.taskgraph._task:
            tasks: List[Task] = self._topology.taskgraph._task[vertex]
            for task in tasks:
                task.shutdown()
                
    def _get_executable_task(self):
        pass
    
    def _start_task_execution(self):
        scheduler_log = None
        with open(self._outpath, 'rb') as f:
            scheduler_log = pkl.load(f)
        reschedule_count = 1
        
        nodes = self._cluster.nodes
        executable: List[Worker] = []
        worker_to_node: Dict[Worker, PhysicalNode] = {}
        task_to_worker: Dict[Task, Worker] = {}
        
        # initialization
        for node in nodes:
            for worker in node.worker:
                if worker.assigned:
                    executable.append(worker)
                    worker_to_node[worker] = node

                    for task in worker.graph.task:
                        task_to_worker[task] = worker
        
        # 이 부분 전면적으로 수정 필요
        source_worker, rest_worker = self._get_source_worker(executable)
        ordered_task = self._order_operator_task(rest_worker)
        sink_task:List[SinkTask] = self._get_sink_task()

        interval = 0
        while SystemClock.CURRENT < self._simulation_time:
            # Communication Modeling
            SystemClock.CURRENT += self._freq
            self._network.complete()

            # start source
            for worker in source_worker:
                for task in worker.graph.task:
                    res = task.start()
                    if res != None:
                        edge: List[Task] = worker.graph.edge[task]['target']
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
                        
                        transmission_delay_ms = Network.DISTRIBUTION[type].next()                            
                        res['msg'].update_transmission_delay(transmission_delay_ms)
                        res['msg'].update_accumulated_latency(transmission_delay_ms)
                        res['msg'].update_receive_time(res['msg'].event_time + (transmission_delay_ms / 1000))
                        
                        self._network.route(task, target, res['msg'])
            
            # start operators
            for key in ordered_task:
                for task in ordered_task[key]:
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
                    
                            transmission_delay_ms = Network.DISTRIBUTION[type].next()
                            msg.update_transmission_delay(transmission_delay_ms)
                            msg.update_accumulated_latency(transmission_delay_ms)
                            msg.update_receive_time(msg.event_time + (transmission_delay_ms / 1000))
                            self._network.route(task, target, msg)   
                            
            # start sink
            for task in sink_task:
                task.start()
                
            # report
            q, r = divmod(int(SystemClock.CURRENT), self._period)
            if q != interval and r == 0:
                interval = q
                
                print(f"Now, {SystemClock.CURRENT} (seconds)")
                
                for worker in source_worker:
                    for task in worker.graph.task:
                        task.post_result()
                
                for key in ordered_task:
                    for task in ordered_task[key]:
                        res = task.post_result()
                        self._profiler.update_arvtime(task.id, task.vertex_id, res['interarrival_time']['mean'], res['interarrival_time']['var'])
                        self._profiler.update_srvtime(task.id, task.vertex_id, res['service_time']['mean'], res['service_time']['var'])
                
                for task in sink_task:
                    task.post_result()
                    
                rescale = self._profiler.periodical_update()
                
                if rescale:
                    self._profiler.rescale(self._topology)
                    stime = datetime.now()
                    fitness= self._scheduler.reschedule(self._cluster, self._topology)
                    etime = datetime.now()
                    
                    nodes = self._cluster.nodes
                    executable: List[Worker] = []
                    worker_to_node: Dict[Worker, PhysicalNode] = {}
                    task_to_worker: Dict[Task, Worker] = {}
                    for node in nodes:
                        for worker in node.worker:
                            if worker.assigned:
                                executable.append(worker)
                                worker_to_node[worker] = node

                                for task in worker.graph.task:
                                    task_to_worker[task] = worker
                    
                    source_worker, rest_worker = self._get_source_worker(executable)
                    ordered_task = self._order_operator_task(rest_worker)
                    sink_task:List[SinkTask] = self._get_sink_task()
                    
                    scheduler_log[reschedule_count] = {
                        reschedule_count: {
                            'event_time': str(SystemClock.CURRENT),
                            'stime': str(stime),
                            'etime': str(etime),
                            'elapsed_time': str(etime - stime),
                            'scheduler': self._scheduler.__class__.__name__,
                            'cluster_size': len(self._cluster.nodes),
                            'subgraph_size': len(self._topology.taskgraph.subgraph),
                            'fitness': fitness
                        }
                    }
                    reschedule_count += 1
                
                print('-'*50)             
        
    
    
    def start_benchmark(self):
        print(f'Start {self._scheduler.__class__.__name__} benchmark')
        self._start_scheduling()
        print(f'Start Tasks of {self._scheduler.__class__.__name__}')
        self._start_task_execution()
        self._shutdown_task()
        print(f'Finish Tasks of {self._scheduler.__class__.__name__}')
        print(f'Finish {self._scheduler.__class__.__name__} benchmark')
        
        SystemClock.CURRENT = 0