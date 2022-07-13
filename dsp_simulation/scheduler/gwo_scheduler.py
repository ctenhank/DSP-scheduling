from copy import deepcopy
import sys
from typing import Dict, List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.scheduler import MetaHueristicScheduler
from dsp_simulation.scheduler.objective import  Objective
from dsp_simulation.topology.topology import Topology
import random as rd
import numpy as np

class Wolf:
    def __init__(self, available_worker: List[PhysicalNode], num_choice:int, seed: int):
        self._rnd = rd.seed(seed)
        #self._assignment: List[PhysicalNode] = self._initialize_individual(cluster, topology)
        self._worker = available_worker
        self.assignment: np.array = self._initialize_individual(len(available_worker), num_choice)
        self._raw_fitness = self._get_seperate_fitness()
        self.fitness = 0.0
    
    #@property
    #def assignment(self):
    #    return self.assignment
    
    #@property
    #def fitness(self):
    #    return self._fitness
    
    @property
    def raw_fitness(self):
        return self._raw_fitness
    
    def update_fitness_by_min_max(self, min: Dict[str, float], max: Dict[str,float]):
        """Current supporting keys of objectives are ['network', 'failure']

        Args:
            min (Dict[str, float]): _description_
            max (Dict[str,float]): _description_
        """
        if len(min.keys()) != len(max.keys()):
            print(f'The size of min and max dictionary are different')
            return
        
        num_objectives = len(min.keys())
        fair_weight = 1 / num_objectives
        for key in min:
            subtraction = (max[key] - min[key])
            if subtraction == 0:
                subtraction = 1
            #self.fitness += fair_weight * (self._raw_fitness[key] - min[key])/ (subtraction)
            self.fitness += fair_weight * self._raw_fitness[key]
    
    def _get_seperate_fitness(self):
        assignment = [self._worker[idx] for idx, choice in enumerate(self.assignment) if choice]
        #print(assignment)
        network = Objective.topology_network_distance(assignment)
        #failure = Objective.system_failure(assignment)
        failure = Objective.availability(assignment)
        
        return {
            'network': network,
            'failure': failure
            }
    
    def _initialize_individual(self, num_worker: int, num_choice: int) -> np.array:
        """Select randomly the nodes of cluster to allocate the topology

        Returns:
            List[PhysicalNode]: _description_
        """
        ret = np.zeros(num_worker)
        while num_choice:
            idx = rd.randint(0, num_worker-1)
            while ret[idx]:
                idx = rd.randint(0, num_worker-1)
            
            ret[idx] = 1
            num_choice -= 1
        return ret


class GWOScheduler(MetaHueristicScheduler):
    """Grey Wolf Optimization algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """
    def __init__(self, num_wolves: int, num_iter: int=50):
        super().__init__(f'{__class__.__name__}_{num_wolves}_{num_iter}')
        self._num_wolves = num_wolves
        self._max_iteration = num_iter
        
    def _update_fitness(self, wolves: List[Wolf]):
        self._minimum = {
            'network': sys.maxsize,
            'failure': sys.maxsize
        }
        
        self._maximum = {
            'network': 0,
            'failure': 0
        }
        
        for wolf in wolves:
            self._minimum['network'] = min(self._minimum['network'], wolf.raw_fitness['network'])
            self._maximum['network'] = max(self._maximum['network'], wolf.raw_fitness['network'])
            self._minimum['failure'] = min(self._minimum['failure'], wolf.raw_fitness['failure'])
            self._maximum['failure'] = max(self._maximum['failure'], wolf.raw_fitness['failure'])
            
        for wolf in wolves:
            wolf.update_fitness_by_min_max(min=self._minimum, max=self._maximum)
            
    def _initialize_environment(self, cluster: Cluster):
        self._worker_to_node: List[PhysicalNode] = []
        for node in cluster.nodes:
            for _ in node.get_available_worker():
                self._worker_to_node.append(node)
    
    def repair(self, x: np.array):
        cnt = 0
        for i in x:
            if x[i] == 1:
                x[i] = 0
            else:
                x[i] = 1
                cnt += 1
        pass
    
    def _get_seperate_fitness(self, x):
        assignment = [self._worker_to_node[idx] for idx, choice in enumerate(x) if choice]
        #print(assignment)
        network = Objective.topology_network_distance(assignment)
        #failure = Objective.system_failure(assignment)
        failure = Objective.availability(assignment)
        
        return {
            'network': network,
            'failure': failure
            }
    
    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:        
        #wolves = [Wolf(
        #    cluster=cluster,
        #    topology=topology,
        #    seed=i) for i in range(self._num_wolves) ]
        
        wolves = [Wolf(
            available_worker=self._worker_to_node,
            num_choice=len(topology.taskgraph.subgraph),
            seed=i) for i in range(self._num_wolves) ]
        self._update_fitness(wolves)
        wolves = sorted(wolves, key=lambda wolf: wolf.fitness)
        alpha, beta, gamma = deepcopy(wolves[:3])
        #print(alpha.fitness, beta.fitness, gamma.fitness)
        len_graph = len(topology.taskgraph.subgraph)
        len_worker = len(self._worker_to_node)
        
        
        iteration = 0
        while iteration < self._max_iteration:
            #print("Iter = " + str(iteration) + " best fitness = %.3f" % alpha.fitness)
            
            w = np.array([[alpha.fitness, beta.fitness, gamma.fitness]]) / (alpha.fitness + beta.fitness + gamma.fitness)
            u = np.exp(-100 * iteration / self._max_iteration)
            u = 2 * (1 - iteration / self._max_iteration)
            cat = np.concatenate((alpha.assignment.reshape(1, len_worker), beta.assignment.reshape(1, len_worker), gamma.assignment.reshape(1, len_worker)), axis=0)
            xp = (np.matmul(w, cat) + u * np.random.normal(size=(1, len_worker))).reshape(len_worker)
            
            
            for k in range(self._num_wolves):
                x = np.zeros(len_worker)                    

                for j in range(len_graph):
                    if np.count_nonzero(x == 1) >= len_graph:
                        break
                    
                    r = 2 * ( 2 * rd.random() - 1)
                    y = xp[j] - r * np.abs(xp[j] - wolves[k].assignment[j])
                    r = np.abs(np.tanh(y))
                    
                    if rd.random() < r:
                        x[j] = 1
                    else:
                        x[j] = 0
                
                        
                if np.count_nonzero(x == 1) < len_graph:
                    cnt = len_graph - np.count_nonzero(x == 1)
                    while cnt:
                        idx = rd.randint(0, len_worker-1)
                        while x[idx]:
                            idx = rd.randint(0, len_worker-1)
                        x[idx] = 1
                        cnt -= 1
                        
                res = self._get_seperate_fitness(x)
                self._minimum['network'] = min(self._minimum['network'], res['network'])
                self._maximum['network'] = max(self._maximum['network'], res['network'])
                self._minimum['failure'] = min(self._minimum['failure'], res['failure'])
                self._maximum['failure'] = max(self._maximum['failure'], res['failure'])
                
                new_fitness = 0
                num_objectives = len(res.keys())
                fair_weight = 1 / num_objectives
                for key in res:
                    subtraction = (self._maximum[key] - self._minimum[key])
                    if subtraction == 0:
                        subtraction = 1
                    new_fitness += fair_weight * res[key]
  
                if new_fitness < wolves[k].fitness:
                    wolves[k].fitness = new_fitness
                    wolves[k].assignment = x
                
            wolves = sorted(wolves, key=lambda wolf: wolf.fitness)
            alpha, beta, gamma = deepcopy(wolves[:3])                
            
            iteration += 1
        
        return alpha.assignment
    
    
        
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        if not self.canSchedule(cluster, topology):
            return False
        
        self._initialize_environment(cluster)
        best = self._meta_algorithm(cluster, topology)
        
        if best is None:
            return False
        
        assignment = [self._worker_to_node[idx] for idx, choice in enumerate(best) if choice]
        
        del self._worker_to_node
        del self._minimum
        del self._maximum
        
        return assignment