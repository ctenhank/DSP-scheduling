from datetime import datetime
from typing import Dict, List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.network import Network
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.cluster.worker import Worker
from dsp_simulation.etc.clock import SystemClock
from dsp_simulation.runtime.profiler import Profiler
from dsp_simulation.runtime.reporter import Reporter
from dsp_simulation.scheduler.objective import Objective
from dsp_simulation.scheduler.scheduler import Scheduler
from dsp_simulation.topology.task import OperatorTask, SinkTask, Task
from dsp_simulation.topology.topology import Topology
import pickle as pkl
from pathlib import Path
import pandas as pd
import random as rd
import dsp_simulation.topology.task as t                
import time
        

class Simulator:
    def __init__(self, cluster: Cluster, topology: Topology, scheduler: Scheduler, profiler: Profiler, outdir='./data/', type='wc',  tot_time=900, time_freq=10000, period=1, runtime:bool=False):
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
        self._reporter = Reporter(self._cluster)
        self._outpath: Path = Path(outdir) / self._scheduler.id
        self._outpath.mkdir(exist_ok=True, parents=True)
        self._simulation_time = tot_time
        self._distribution = None
        self._freq = 1 / time_freq
        self._network = Network()
        self._period = period
        self._runtime_support = runtime
        self._should_rebalance = False
        self._reschedule_time = 0
        self._reschedule_elapsed_time = 0
        self._future_assignment = None
        self._source_current_sent_msg = {}
        self._last_second = 0
        
    
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
            if tier in order and now in order[tier]:
                tier += 1
                
            for next in graph[now]:
                if next not in visited:
                    if tier not in order:
                        order[tier] = []
                    
                    order[tier].append(next)
                    q.append(next)
                visited.append(next)

        
        #while q:
        #    now = q.pop(0)
        #    if now in order[tier]:
        #        tier += 1
                
        #    for next in graph[now]:
        #        if next not in visited:
        #            if tier not in order:
        #                order[tier] = []
                    
        #            order[tier].append(next)
        #            q.append(next)
        #        visited.append(next)
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
        #elasped_time = str(datetime.now() - stime)
        etime = datetime.now()
        
        #if 'Scheduler' in t.outdir.stem:
        #    t.outdir = t.outdir.parent
        #t.outdir = t.outdir / self._scheduler.__class__.__name__
        print(f'{self._scheduler.__class__.__name__}-{0}th: {Objective.availability(assignment)}')
        self._scheduler_log = {
            0:{
                'event_time': str(SystemClock.CURRENT),
                'stime': stime,
                'etime': etime,
                'elapsed_time': str(etime - stime),
                'scheduler': self._scheduler.__class__.__name__,
                'cluster_size': len(self._cluster.nodes),
                'subgraph_size': len(self._topology.taskgraph.subgraph),
                'fitness_network': Objective.topology_network_distance(assignment),
                #'fitness_failure': Objective.system_failure(assignment),
                'fitness_failure': Objective.availability(assignment),
                }
        }
        
        #with open(self._outpath, 'wb') as f:
        #    pkl.dump(self._scheduler_log, f)
            
    def _shutdown_task(self):
        for vertex in self._topology.taskgraph._task:
            tasks: List[Task] = self._topology.taskgraph._task[vertex]
            for task in tasks:
                task.shutdown(str(self._outpath))
                
        with open(str(self._outpath / 'scheduler.pkl'), 'wb') as f:
            pkl.dump(self._scheduler_log, f)
            
        self._reporter.shutdown(str(self._outpath / 'reporter.pkl'))
            
    def _get_executable_task(self):
        pass
    
    def _start_task_execution(self):
        #scheduler_log = None
        #with open(self._outpath, 'rb') as f:
        #    scheduler_log = pkl.load(f)
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
            
            if self._should_rebalance and SystemClock.CURRENT >= self._reschedule_time:
                self._should_rebalance = False
                self._reschedule_time = 0.0
                
                for worker in source_worker:
                    for task in worker.graph.task:
                        self._source_current_sent_msg[task.vertex_id] = task.sent_msg_cnt_period
                
                self._cluster.assign_topology(self._topology, self._future_assignment)
                
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
                
                print(f'{self._scheduler.__class__.__name__}-{reschedule_count}th: {Objective.availability(self._future_assignment)}')
                self._scheduler_log[reschedule_count] = {
                        'event_time': str(SystemClock.CURRENT),
                        'elapsed_time': str(self._reschedule_elapsed_time),
                        'scheduler': self._scheduler.__class__.__name__,
                        'cluster_size': len(self._cluster.nodes),
                        'subgraph_size': len(self._topology.taskgraph.subgraph),
                        'fitness_network': Objective.topology_network_distance(self._future_assignment),
                        'fitness_failure': Objective.availability(self._future_assignment),
                }
                reschedule_count += 1
                self._network.initialize()
                
                previous = self._last_second
                print(f'previous: {previous}, current: {SystemClock.CURRENT}')
                cnt11 = 0
                src_cnt = 0
                sink_cnt = 0
                op_cnt = {}
                while previous < SystemClock.CURRENT:
                    cnt11 +=1
                    previous += self._freq
                    self._network.fake_complete(previous)
                    
                    for worker in source_worker:
                        for task in worker.graph.task:
                            res = task.fake_start(previous)
                            if res != None:
                                src_cnt += 1
                                for destination in worker.graph.edge[task]['target']:
                                    edge: List[Task] = worker.graph.edge[task]['target'][destination]
                                    target = rd.choice(edge)
                                    
                                    if target not in task_to_worker:
                                        continue
                                    
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
                            res = task.fake_start(previous)
                            if res != None:
                                if task.vertex_id not in op_cnt:
                                    op_cnt[task.vertex_id] = 0
                                op_cnt[task.vertex_id] += 1 
                                for msg in res['msg']:
                                    for destination in task_to_worker[task].graph.edge[task]['target']:
                                        edge = task_to_worker[task].graph.edge[task]['target'][destination]
                                        target = rd.choice(edge)
                                        
                                        if target not in task_to_worker:
                                            continue
                                            
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
                                    #edge = task_to_worker[task].graph.edge[task]['target']
                                    #target = rd.choice(edge)
                                        
                                    #target_worker = task_to_worker[target]
                                    #source_node = worker_to_node[worker]
                                    #target_node = worker_to_node[target_worker]
                                    
                                    #type = Network.TYPE[0]
                                    #if source_node.id != target_node.id:
                                    #    if source_node.rack != target_node.rack:
                                    #        type = Network.TYPE[3]
                                    #    else:
                                    #        type = Network.TYPE[2]
                                    
                                    #if worker.id != target_worker.id:
                                    #    type = Network.TYPE[1]
                            
                                    #transmission_delay_ms = Network.DISTRIBUTION[type].next()
                                    #msg.update_transmission_delay(transmission_delay_ms)
                                    #msg.update_accumulated_latency(transmission_delay_ms)
                                    #msg.update_receive_time(msg.event_time + (transmission_delay_ms / 1000))
                                    #self._network.route(task, target, msg)   
                                    
                    # start sink
                    for task in sink_task:
                        sink_cnt += 1
                        task.start()
                
                #print(cnt11, src_cnt, sink_cnt)
                #for key in op_cnt:
                #    print(f'{key}: {op_cnt[key]}')
                cnt11 = 0
                src_cnt = 0 
                sink_cnt = 0
                op_cnt = {}
                

            # start source
            for worker in source_worker:
                for task in worker.graph.task:
                    res = task.start()
                    if res != None:
                        for destination in worker.graph.edge[task]['target']:
                            edge: List[Task] = worker.graph.edge[task]['target'][destination]
                            target = rd.choice(edge)
                            
                            if target not in task_to_worker:
                                        continue
                            
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
                        #edge: List[Task] = worker.graph.edge[task]['target']
                        #target = rd.choice(edge)
                        
                        #target_worker = task_to_worker[target]
                        #source_node = worker_to_node[worker]
                        #target_node = worker_to_node[target_worker]
                        
                        #type = Network.TYPE[0]
                        #if source_node.id != target_node.id:
                        #    if source_node.rack != target_node.rack:
                        #        type = Network.TYPE[3]
                        #    else:
                        #        type = Network.TYPE[2]
                        
                        #if worker.id != target_worker.id:
                        #    type = Network.TYPE[1]
                        
                        #transmission_delay_ms = Network.DISTRIBUTION[type].next()                            
                        #res['msg'].update_transmission_delay(transmission_delay_ms)
                        #res['msg'].update_accumulated_latency(transmission_delay_ms)
                        #res['msg'].update_receive_time(res['msg'].event_time + (transmission_delay_ms / 1000))
                        
                        #self._network.route(task, target, res['msg'])
            
            # start operators
            for key in ordered_task:
                for task in ordered_task[key]:
                    res = task.start()
                    if res != None:
                        for msg in res['msg']:
                            for destination in task_to_worker[task].graph.edge[task]['target']:
                                edge = task_to_worker[task].graph.edge[task]['target'][destination]
                                target = rd.choice(edge)
                                
                                if target not in task_to_worker:
                                        continue
                                    
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
                            #edge = task_to_worker[task].graph.edge[task]['target']
                            #print(f'{task.vertex_id} target: {task_to_worker[task].graph.edge[task]["target"]}')
                            #print(f"{task.vertex_id}: {task_to_worker[task].graph.edge[task]}")
                            #for target in task_to_worker[task].graph.edge[task]['target']:
                            #    print(f'{target.id}: {target.vertex_id}')
                                
                            #exit(1)
                            #target = rd.choice(edge)
                                
                            #target_worker = task_to_worker[target]
                            #source_node = worker_to_node[worker]
                            #target_node = worker_to_node[target_worker]
                            
                            #type = Network.TYPE[0]
                            #if source_node.id != target_node.id:
                            #    if source_node.rack != target_node.rack:
                            #        type = Network.TYPE[3]
                            #    else:
                            #        type = Network.TYPE[2]
                            
                            #if worker.id != target_worker.id:
                            #    type = Network.TYPE[1]
                    
                            #transmission_delay_ms = Network.DISTRIBUTION[type].next()
                            #msg.update_transmission_delay(transmission_delay_ms)
                            #msg.update_accumulated_latency(transmission_delay_ms)
                            #msg.update_receive_time(msg.event_time + (transmission_delay_ms / 1000))
                            #self._network.route(task, target, msg)   
                            
            # start sink
            for task in sink_task:
                task.start()
                
            # report
            q, r = divmod(int(SystemClock.CURRENT), self._period)
            if q != interval and r == 0:
                interval = q
                self._last_second = SystemClock.CURRENT
                
                print(f"Now, {SystemClock.CURRENT} (seconds)")
                
                for worker in source_worker:
                    for task in worker.graph.task:
                        res = task.post_result()
                        self._reporter.update_stats(self._topology.id, task, res['reporter'])
                
                for key in ordered_task:
                    for task in ordered_task[key]:
                        res = task.post_result()
                        if self._runtime_support:
                            self._profiler.update_arvtime(task.id, task.vertex_id, res['profiler']['interarrival_time']['mean'], res['profiler']['interarrival_time']['var'])
                            self._profiler.update_srvtime(task.id, task.vertex_id, res['profiler']['service_time']['mean'], res['profiler']['service_time']['var'])
                        self._reporter.update_stats(self._topology.id, task, res['reporter'])
                
                for task in sink_task:
                    res = task.post_result()
                    self._reporter.update_stats(self._topology.id, task, res['reporter'])
                
                self._reporter.report()
                
                if self._runtime_support and not self._should_rebalance:
                    rescale = self._profiler.periodical_update()
                    print(rescale)
                
                if self._runtime_support and rescale:
                    rescale = False
                    #print(SystemClock.CURRENT)
                    self._should_rebalance = True
                    #for worker in source_worker:
                    #    for task in worker.graph.task:
                    #        self._source_current_sent_msg[task.vertex_id] = task.sent_msg_cnt_period
                    self._profiler.rescale(self._topology)
                    #stime = datetime.now()
                    stime2 = time.time_ns()
                    self._cluster, assignment, self._reschedule_elapsed_time, elapsed_time = self._scheduler.reschedule(self._cluster, self._topology)
                    self._future_assignment = assignment
                    etime2 = time.time_ns()
                    #elapsed_time = etime2 - stime2
                    self._reschedule_time = SystemClock.CURRENT + elapsed_time/10**(9)
                    
                    #print(type(SystemClock.CURRENT))
                    #print(type(self._reschedule_time))
                    #self._cluster.assign_topology(self._topology, assignment)
                    #etime = datetime.now()
                    #self._reschedule_elapsed_time = str(etime- stime)
                    print(f'rescheduling time: {self._reschedule_elapsed_time}')
                    
                    
                    #nodes = self._cluster.nodes
                    #executable: List[Worker] = []
                    #worker_to_node: Dict[Worker, PhysicalNode] = {}
                    #task_to_worker: Dict[Task, Worker] = {}
                    #for node in nodes:
                    #    for worker in node.worker:
                    #        if worker.assigned:
                    #            executable.append(worker)
                    #            worker_to_node[worker] = node

                    #            for task in worker.graph.task:
                    #                task_to_worker[task] = worker
                    
                    #source_worker, rest_worker = self._get_source_worker(executable)
                    #ordered_task = self._order_operator_task(rest_worker)
                    #sink_task:List[SinkTask] = self._get_sink_task()
                    
                    #self._scheduler_log[reschedule_count] = {
                    #        'event_time': str(SystemClock.CURRENT),
                    #        'stime': str(stime),
                    #        'etime': str(etime),
                    #        'elapsed_time': str(etime - stime),
                    #        'scheduler': self._scheduler.__class__.__name__,
                    #        'cluster_size': len(self._cluster.nodes),
                    #        'subgraph_size': len(self._topology.taskgraph.subgraph),
                    #        'fitness_network': Objective.topology_network_distance(assignment),
                    #        'fitness_failure': Objective.system_failure(assignment),
                    #        #'fitness': fitness11``
                    #}
                    #reschedule_count += 1
                
                print('-'*50)             
        
    
    
    def start_benchmark(self):
        print(f'Start {self._scheduler.id} benchmark')
        self._start_scheduling()
        print(f'Start Tasks of {self._scheduler.id}')
        self._start_task_execution()
        self._shutdown_task()
        print(f'Finish Tasks of {self._scheduler.id}')
        print(f'Finish {self._scheduler.id} benchmark')
        
        SystemClock.CURRENT = 0