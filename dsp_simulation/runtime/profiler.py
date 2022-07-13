from math import sqrt
from typing import Dict, List

from dsp_simulation.topology.topology import Topology
from dsp_simulation.cluster.cluster import Cluster

class Profiler:
    def __init__(self, cluster: Cluster, topology: Topology):
        self._cluster = cluster
        self._topology = topology
        
        self._first_update = True
        
        self.task_srvtime: Dict = {}
        self.task_arvtime: Dict = {}
        self.task_linktime: Dict = {}
        
        # S_{jv}, Var(S_{jv})
        self.vertex_mean_srvtime: Dict[str, List[float]] = {}
        self.vertex_var_srvtime: Dict[str, List[float]] = {}
        # A_{jv}, Var(A_{jv})
        self.vertex_mean_arvtime: Dict[str, List[float]] = {}
        self.vertex_var_arvtime: Dict[str, List[float]] = {}
        
        # W^{K}_{jv}
        self.vertex_kingman: Dict[str, List[float]] = {}
        
        # Thresholds related to the bottleneck
        self._max_threshold_utilization = 1.0
        self._min_threshold_utilization = 0.5
        self._bottleneck_threshold = 1.0

        
    def update_srvtime(self, task_id: str, vertex_id, mean, var):
        if task_id not in self.task_srvtime:
            self.task_srvtime[task_id] = {
                'vertex_id': vertex_id,
                'mean': [mean],
                'var': [var]
            }
        else:
            self.task_srvtime[task_id]['mean'].append(mean)
            self.task_srvtime[task_id]['var'].append(var)
    
    def update_arvtime(self, task_id: str, vertex_id, mean, var):
        if task_id not in self.task_arvtime:
            self.task_arvtime[task_id] = {
                'vertex_id': vertex_id,
                'mean': [mean],
                'var': [var]
            }
        else:
            self.task_arvtime[task_id]['mean'].append(mean)
            self.task_arvtime[task_id]['var'].append(var)
            
    def update_vertex_order(self, vertex_order):
        #self._vertex_order.append(vertex_order)
        self._vertex_order = vertex_order
    
    def _coefficient_variation(self, calculation_type, vertex_id):
        ret = None
        if calculation_type == 'srv':
            ret = sqrt(self.vertex_var_srvtime[vertex_id][-1]) / self.vertex_mean_srvtime[vertex_id][-1]
        elif calculation_type == 'arv':
            ret = sqrt(self.vertex_var_arvtime[vertex_id][-1]) / self.vertex_mean_arvtime[vertex_id][-1]
        return ret
    
    def _arrival_rate(self, vertex_id):
        return 1 / self.vertex_mean_arvtime[vertex_id][-1]
    
    def _service_rate(self, vertex_id):
        return 1 / self.vertex_mean_srvtime[vertex_id][-1]
    
    def _utilization(self, vertex_id, arrival_rate=None):
        
        
        if arrival_rate != None:
            #print(f'{vertex_id}: {arrival_rate}, {self.vertex_mean_srvtime[vertex_id][-1]}')
            return arrival_rate * self.vertex_mean_srvtime[vertex_id][-1]
        #print(f'{self._arrival_rate(vertex_id)}: {arrival_rate}, {self.vertex_mean_srvtime[vertex_id][-1]}')
        return self._arrival_rate(vertex_id) * self.vertex_mean_srvtime[vertex_id][-1]
    
    def _kingman(self, vertex_id):
        return ((self._utilization(vertex_id)/self._service_rate(vertex_id))/abs(1 - self._utilization(vertex_id)))*((self._coefficient_variation('srv', vertex_id)**2 + self._coefficient_variation('arv', vertex_id)**2) / 2)      
    
    def _current_parallelism(self, vertex_id):
        return len(self.vertex_to_task[vertex_id])
    
    def _estimate_parallelism(self, vertex_id, arrival_rate=None, min=1, max=200):
        # 현재 arrival rate와 
        for par in range(min, max):
            util = self._utilization(vertex_id, arrival_rate) * (self._current_parallelism(vertex_id) / par)
            #print(f'{vertex_id}: {par}, {util}')
            if util <= self._max_threshold_utilization and util >= self._min_threshold_utilization:
                
                return par
                #return int(par * 1.5)
        return min
    
    
    def _has_bottleneck(self, topology: Topology):
        ret = []
        for vertex_id in self.vertex_to_task:
            if self._utilization(vertex_id) >= self._bottleneck_threshold:
                ret.append(vertex_id)
        return ret
    
    def _merge_dictionary_by_max(self, d1: dict, d2: dict):
        for key in d2.keys():
            if key not in d1:
                d1[key] = d2[key]
            else:
                d1[key] = max(d1[key], d2[key])
        return d1
    
    def _resolve_bottleneck(self, vertex_id: str, output_rate=None, first=True):
        """resolve bottleneck by increasing the parallelism of given vertex and propagate its effect to the connected vertices based on the output rate

        Args:
            vertex_id (str): vertex id
            output_rate (_type_, optional): input rate of previous operator
        """
        ret = {}
        if output_rate == None:
            output_rate = self._arrival_rate(vertex_id=vertex_id) * self._topology.get_vertex(vertex_id).selectivity
            
        if first:
            estimated = self._estimate_parallelism(vertex_id)
        else:
            estimated = self._estimate_parallelism(vertex_id, output_rate)

        if first:
            ret[vertex_id] = int(estimated * 1.5)
            first = False
        else:
            ret[vertex_id] = estimated

        target = self._topology.get_target(vertex_id)
        for v in target:
            res = self._resolve_bottleneck(v, output_rate * self._topology.get_vertex(v).selectivity, first)
            ret = self._merge_dictionary_by_max(ret, res)

        return ret
    

    def periodical_update(self):
        if self._first_update:                
            self.vertex_to_task: Dict = {}
            for task_id in self.task_srvtime:
                vertex_id = self.task_srvtime[task_id]['vertex_id']
                if vertex_id not in self.vertex_to_task:
                    self.vertex_to_task[vertex_id] = [task_id]
                    self.vertex_kingman[vertex_id] = []
                    self.vertex_mean_arvtime[vertex_id] = []
                    self.vertex_var_arvtime[vertex_id] = []
                    self.vertex_mean_srvtime[vertex_id] = []
                    self.vertex_var_srvtime[vertex_id] = []
                else:
                    self.vertex_to_task[vertex_id].append(task_id)
            self._first_update = False
            
        for vertex_id in self.vertex_to_task:
            mean_srvtime = 0
            mean_arvtime = 0
            for task_id in self.vertex_to_task[vertex_id]:
                mean_srvtime += self.task_srvtime[task_id]['mean'][-1]
                mean_arvtime += self.task_arvtime[task_id]['mean'][-1]
            mean_srvtime *= 1 / len(self.vertex_to_task[vertex_id])
            mean_arvtime *= 1 / len(self.vertex_to_task[vertex_id])
            
            self.vertex_mean_srvtime[vertex_id].append(mean_srvtime)
            self.vertex_mean_arvtime[vertex_id].append(mean_arvtime)
            
            var_srvtime = 0
            var_arvtime = 0
            for task_id in self.vertex_to_task[vertex_id]:
                var_srvtime += self.task_srvtime[task_id]['var'][-1]
                var_arvtime += self.task_arvtime[task_id]['var'][-1]
                #var_srvtime += np.array(self.task_srvtime[task_id]['var']).mean()
                #var_arvtime += np.array(self.task_arvtime[task_id]['var']).mean()
            var_srvtime *= 1 / len(self.vertex_to_task[vertex_id])
            var_arvtime *= 1 / len(self.vertex_to_task[vertex_id])
            
            self.vertex_var_srvtime[vertex_id].append(var_srvtime)
            self.vertex_var_arvtime[vertex_id].append(var_arvtime)
        
        for vertex_id in self.vertex_to_task:
            self.vertex_kingman[vertex_id].append(self._kingman(vertex_id))   
        
        overutilization = 0
        for vertex_id in self.vertex_to_task:
            #print(f'{vertex_id} Mean arrival rate: {1 / self.vertex_mean_arvtime[vertex_id][-1]}')
            #print(f'{vertex_id} Service rate: {1 / self.vertex_mean_srvtime[vertex_id][-1]}')
            #print(f'{vertex_id} Traffic intensity: {self.vertex_mean_srvtime[vertex_id][-1] / self.vertex_mean_arvtime[vertex_id][-1]}')
            #print(f'{vertex_id} CV of arrival time: {self._coefficient_variation("arv", vertex_id)}')
            #print(f'{vertex_id} CV of service time: {self._coefficient_variation("srv", vertex_id)}')
            #print(f'{vertex_id} avg queue waiting time(ms): {self.vertex_kingman[vertex_id][-1] * 1000}')
            #print(f'{vertex_id} utilization: {self._utilization(vertex_id)}')
            if self._utilization(vertex_id) >= self._bottleneck_threshold:
                #print(self._utilization(vertex_id))
                overutilization += 1
                
        if overutilization >= 1:
            return True
        return False
                
                
    def rescale(self, topology: Topology):
        bottleneck = self._has_bottleneck(topology)
        if bottleneck:
            res = {operator.id:operator.parallelism for operator in topology.operator}
            for vertex_id in bottleneck:
                print(f'Bottleneck {vertex_id}, {self._utilization(vertex_id)}')
                _res = self._resolve_bottleneck(vertex_id)
                res = self._merge_dictionary_by_max(res, _res)
                
            self._first_update = True
            self.task_srvtime: Dict = {}
            self.task_arvtime: Dict = {}
            self.task_linktime: Dict = {}
            
            for operator in topology.operator:
                print(f'operator {operator.id}: {operator.parallelism} -> {res[operator.id]}')
                operator.parallelism = res[operator.id]
                
        
        return topology
