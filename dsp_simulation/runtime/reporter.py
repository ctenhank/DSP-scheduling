from pathlib import Path
from typing import Dict, List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.topology.task import OperatorTask, SinkTask, SourceTask, Task
import numpy as np
import pickle as pkl

class Reporter:
    def __init__(self, cluster: Cluster):
        self._stats = {}
        self._operator_stats = {}
        self._vertex: Dict[str, List[str]] = {}
        self._cluster = cluster
    
    def update_stats(self, topology_id:str, task: Task, stats: dict):
        if topology_id not in self._stats:
            self._stats[topology_id] = {}
            self._operator_stats[topology_id] = {}
        
        topo_stats = self._stats[topology_id]
        op_stats = self._operator_stats[topology_id]
        
        if type(task) == SourceTask:
            vid = 'src-' + task.vertex_id
            if vid not in topo_stats:    
                topo_stats[vid] = {
                    'sent_msg_cnt': []
                }
            topo_stats[vid]['sent_msg_cnt'].append(stats['sent_msg_cnt'])
            
        elif type(task) == OperatorTask:
            vid = 'op-' + task.vertex_id
            if vid not in topo_stats:    
                topo_stats[vid] = {
                    'throughput': [],
                    'processing_latency': [],
                    'execute_latency': []
                }
                
                op_stats[vid] = {
                    'throughput': [],
                    'processing_latency': [],
                    'execute_latency': []
                }
                
            op_stats[vid]['throughput'].append(stats['throughput'])
            op_stats[vid]['processing_latency'].append(stats['processing_latency'])
            op_stats[vid]['execute_latency'].append(stats['execute_latency'])
            
        elif type(task) == SinkTask:
            vid = 'sink-' + task.vertex_id
            if vid not in topo_stats:    
                topo_stats[vid] = {
                    'throughput': [],
                    'end_to_end_delay': []
                }
            topo_stats[vid]['throughput'].append(stats['throughput'])
            topo_stats[vid]['end_to_end_delay'].append(stats['end_to_end_delay'])
    
    def _get_topology_info(self, topology_id: str):
        target = None
        for topology in self._cluster.topology:
            if topology.id == topology_id:
                target = topology
                break
            
        info = {'topology_size': len(topology.taskgraph.subgraph)}
        for operator in target.operator:
            info[operator.id + '_parallelism'] = operator.parallelism
        return info
    
    def report(self):
        print('='*50)
        print(f'Cluster: Physical Node #({len(self._cluster.nodes)})')
        
        for topology in self._stats:
            for vertex in self._stats[topology]:
                op_type = vertex.split('-')[0]
                vtx_stats = self._stats[topology][vertex]
                if  op_type == 'op':
                    throughput = sum(self._operator_stats[topology][vertex]['throughput'])
                    processing_latency = np.mean(self._operator_stats[topology][vertex]['processing_latency'])
                    execute_latency = np.mean(self._operator_stats[topology][vertex]['execute_latency'])
                    
                    self._operator_stats[topology][vertex]['throughput'] = []
                    self._operator_stats[topology][vertex]['processing_latency'] = []
                    self._operator_stats[topology][vertex]['execute_latency'] = []
                    
                    vtx_stats['throughput'].append(throughput)
                    vtx_stats['processing_latency'].append(processing_latency)
                    vtx_stats['execute_latency'].append(execute_latency)
                    print(f'OperatorVertex {vertex}: throughput({vtx_stats["throughput"][-1]}), processing_latency({vtx_stats["processing_latency"][-1]}), execute_latency({vtx_stats["execute_latency"][-1]})')
                elif op_type == 'src':
                    print(f'SourceVertex {vertex}: sent message count({vtx_stats["sent_msg_cnt"][-1]})')
                elif op_type == 'sink':
                    print(f'SinkVertex {vertex}: throughput({vtx_stats["throughput"][-1]}), end_to_end_delay({vtx_stats["end_to_end_delay"][-1]})')
            print('-'*50)
        print('='*50)
        
    def shutdown(self, filename:str):
        #print(self._stats)
        with Path(filename).open('wb') as f:
            pkl.dump(self._stats, f)
            
            #f.write('Hello wolrd')
        