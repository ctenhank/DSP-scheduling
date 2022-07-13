from shutil import move
import sys
from typing import List, Tuple
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.objective import Objective, get_network_distance
from dsp_simulation.scheduler.scheduler import MetaHueristicScheduler
from dsp_simulation.topology.topology import Topology
import random as rd
import numpy as np
import multiprocessing as mp
import time
import math

class Ant:
    """An agent finds the best solution in the cluster.
    
    
    Implementation is refered from https://github.com/december0123/Pants
    Returns:
        _type_: _description_
    """
    CNT = 0 
    def __init__(self, size: int):
        """Initialize an ant

        Args:
            current (int): starting node
            size (int): the size of cluster
        """
        
        self._id = 'Ant-' + str(Ant.CNT)
        self._size = size
        self.current = rd.randint(0, self._size - 1)
        self.visited = [self.current]
        self.unvisited = [i for i in range(size) if i != self.current]
        #self.traveled = []
        Ant.CNT += 1
    
    def __str__(self):
        ret = f'{self._id}: '
        return ret

    @property
    def id(self):
        return self._id
    
    def initialize(self):
        self.current = rd.randint(0, self._size - 1)
        self.visited = [self.current]
        self.unvisited = [i for i in range(self._size) if i != self.current]
        self.traveled = []


class ACOScheduler(MetaHueristicScheduler):
    """Ant Colony Optimization algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """

    def __init__(self, id:str=None, num_iter:int = 1, num_ants: int = 200, alpha: float = 3.0, beta: float = 1.0, rho: float = 0.3, Q: float = 1.0, t0: float = 0.1):
        """_summary_

        Args:
            num_ant (_type_): _description_
            alpha (_type_): _description_
            beta (_type_): _description_
            rho (_type_): _description_
            Q (_type_): _description_
        """
        spec = f'_{num_ants}_{num_iter}_{alpha}_{beta}'
        if id is not None:
            super().__init__(id + spec)
        else:
            super().__init__(__class__.__name__ + spec)
        
        self._num_iter = num_iter
        self._num_ants = num_ants
        self._alpha = alpha
        self._beta = beta
        self._rho = rho
        self._Q = Q
        self._tau: List[List[int]] = []
        self._eta: List[List[float]] = []
        self._ants: List[Ant] = []
        self._t0 = t0
        #self._num_solution: int = 0
        self._num_available_workers: int = 0
        self._worker_matrix: dict = {}


    def _initialize_environment(self, cluster: Cluster, topology: Topology):
        """_summary_

        Args:
            topology (Topology): _description_
            cluster (Cluster): _description_
        """
        
        Ant.CNT = 0
        self._ants: List[Ant] = []
        self._tau: List[List[int]] = []
        self._eta: List[List[float]] = []
        self._worker_matrix: dict = {}
        self._num_available_workers: int = 0
        available_nodes = cluster.get_available_physical_node()
        available_matrix = {}
        num_available_workers = 0
        
        # Create a map (worker_idx = :class:PhysicalNode)
        prev, cur = 0, 0
        for node in available_nodes:
            num_worker = 0
            for worker in node.get_available_worker():
                num_worker += 1
            #num_available_workers += node.available_worker_cnt
            
            num_available_workers += num_worker
            cur = num_available_workers
            for i in range(prev, cur):
                available_matrix[i] = node
            prev = cur
        self._num_available_workers = num_available_workers
        self._worker_matrix = available_matrix
        
        #print(f'ACO {num_available_workers}')
        # Initialize the network matrix and pheromone matrix between available workers
        network_matrix = [[0 for _ in range(num_available_workers)] for _ in range(num_available_workers)]
        pheromone_matrix: List[List] = [[0.0 for _ in range(num_available_workers)] for _ in range(num_available_workers)]
        for i in range(num_available_workers):
            for j in range(i, num_available_workers):
                if i != j:
                    #dist = get_network_distance(available_matrix[i], available_matrix[j]) * 0.5 + available_matrix[i].availability * available_matrix[j].availability * 0.5
                    avail = 0
                    if available_matrix[i].id != available_matrix[j].id:
                        avail = math.log(available_matrix[i].availability) + math.log(available_matrix[j].availability)
                    else:
                        avail = math.log(available_matrix[i].availability)
                    dist = get_network_distance(available_matrix[i], available_matrix[j]) * 0.5 + avail * 0.5
                    #dist = Objective.objectvie_weighted_sum(available_matrix[i], available_matrix[j])
                    network_matrix[i][j] = dist
                    network_matrix[j][i] = dist
                    pheromone_matrix[i][j] = self._t0
                    pheromone_matrix[j][i] = self._t0
        self._tau = pheromone_matrix
        self._eta = network_matrix
        
        #print(f'available worker #: {num_available_workers}, subgraph #: {len(topology.taskgraph.subgraph)}')
        
        # Set a starting point where an ant find to other solutions
        for i in range(self._num_ants):
            #starting_point = rd.randint(0, num_available_workers - 1)
            #self._ants.append(Ant(starting_point, num_available_workers))
            self._ants.append(Ant(num_available_workers))
            
        #print(f'available worker #: {self._num_available_workers}')
        #print(f'subgraph length {len(topology.taskgraph.subgraph)}')
            
    
    def _weight(self, cur: int, next: int) -> float:
        #"""Get a weight from current location to the next location by an ant

        #Args:
        #    cur (int): current location
        #    next (int): next location
            
        #Returns:
        #    float: the weight of current to next
        #"""
        
        attractiveness = (1 / self._eta[cur][next]) ** self._alpha
        trail_level = (self._tau[cur][next]) ** self._beta
        return attractiveness * trail_level
    
    def _move_ant(self, ant: Ant):
        """_summary_

        Args:
            ant (Ant): _description_

        Returns:
            _type_: _description_
        """
        
        # Get the every weights from current location to all unvisited location
        weights = []
        for next in ant.unvisited:
            weights.append(self._weight(ant.current, next))
            
        # Calculate the propability for each travel and get one destination randomly
        total_weight = sum(weights)
        cur = ant.current
        #print(ant.unvisited)
        next = np.random.choice(ant.unvisited, p=np.array(weights)/total_weight)
        
        # Change the corresponding varaiables in the ant
        ant.current = next
        ant.visited.append(next)
        ant.unvisited.remove(next)
        return (cur, next)
    
    def _update_global_pheromone(self, movement: List[Tuple[int, int]]):
        """_summary_

        Args:
            cur (int): _description_
            next (int): _description_
        """
        
        local_pheromone = {}
        for m in movement:
            i, j = m[0], m[1]
            
            # swap
            if i > j:
                t = i
                i, j = j, t
            
            if (i, j) not in local_pheromone.keys():
                local_pheromone[(i,j)] = 0
                
            local_pheromone[(i,j)] += self._Q / self._eta[i][j]
            
        for i in range(self._num_available_workers):
            for j in range(self._num_available_workers):
                if i != j:
                    self._tau[i][j] = (1 - self._rho) * self._tau[i][j]
                    if (i, j) in local_pheromone.keys():
                        self._tau[i][j] += local_pheromone[(i, j)]
                        self._tau[j][i] += local_pheromone[(i, j)]
    
    
    def _get_best(self, cluster: Cluster):
        best = sys.maxsize
        ret = []
        
        #available_nodes = cluster.get_available_physical_node()
        #available_worker_cnt = {}
        
        #for node in available_nodes: 
        #    available_worker_cnt[node.id] = node.available_worker_cnt
        
        for ant in self._ants:
            
            assignment = []
            for worker_idx in ant.visited:
                assignment.append(self._worker_matrix[worker_idx])
                
            score = Objective.objectvie_weighted_sum(assignment)
            if best > score:
                best = score
                ret = assignment
                
        return ret
    
    def _get_best(self):
        
        
        #print(f'length of ants: {len(self._ants)}')
        ants_info = {}
        #max_net, min_net = 0, sys.maxsize
        #max_fail, min_fail = 0, sys.maxsize
        nets = []
        fails = []
        #min_net = sys.maxsize
        for ant in self._ants:
            
            assignment = []
            for worker_idx in ant.visited:
                assignment.append(self._worker_matrix[worker_idx])
                
            #score = Objective.objectvie_weighted_sum(assignment)
            #print(f'score: {score}')
            ants_info[ant] = {
                'assignment': assignment,
                'network': Objective.topology_network_distance(assignment=assignment),
                #'failure': Objective.system_failure(assignment)
                'failure': Objective.availability(assignment)
            }
            
            nets.append(ants_info[ant]['network'])
            fails.append(ants_info[ant]['failure'])
            
            #max_net = max(max_net, ants_info[ant]['network'])
            #min_net = min(min_net, ants_info[ant]['network'])
            
            #max_fail = max(max_fail, ants_info[ant]['failure'])
            #min_fail = min(min_fail, ants_info[ant]['failure'])
        
        #print(f'network: {max_net}, {min_net}')
        #print(f'failure: {max_fail}, {min_fail}')
        
        #sub_net = max_net - min_net
        #sub_fail = max_fail - min_fail
        
        #if sub_net == 0:
        #    sub_net = 1
        #if sub_fail == 0:
        #    sub_fail = 1
        
        
        best = sys.maxsize
        #best_not_scaled = 0.0
        #best1 = 0.0 
        #best2 = 0.0
        ret = []
        for idx, ant in enumerate(ants_info):
            ant_info = ants_info[ant]
            #print(ant_info['network'], ant_info['failure'])
            ant_info['fitness'] = self._z_score(nets, idx) * 0.5 + self._z_score(fails, idx)
            
            if best > ant_info['fitness']:
                ##print(f'best so far: {ant_info["fitness"]}')
                
                #best_not_scaled = ant_info['network'] * 0.5 + ant_info['failure'] * 0.5
                #best1 = ant_info['network']
                #best2 = ant_info['failure']
                best = ant_info['fitness']
                #print(f'best fitness: {best}')
                ret = assignment
        
        #print(f'ACO Best not scaled(ants: {self._num_ants}): {best_not_scaled}, {best1}, {best2}')                
        #print(f'ACO Best scaled(ants: {self._num_ants}): {best_not_scaled}, {self._z_score(nets, idx)}, {self._z_score(fails, idx)}')
            
        return ret
                

    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        self._initialize_environment(cluster, topology)
        
        # The number of food has be found by an ant.
        num_solution = len(topology.taskgraph.subgraph) - 1
        
        #while self._num_solution > 1:
            
        #stime = time.time_ns()
        for i in range(self._num_iter):
            for _ in range(num_solution):
                movement: List[Tuple(int, int)] = []
                for ant in self._ants:
                    movement.append(self._move_ant(ant))
                
                self._update_global_pheromone(movement)
            
            if i != self._num_iter -1:
                for ant in self._ants:
                    ant.initialize()
                
        #elapsed_time2 = time.time_ns() - stime
        #print(f'Elpased time2: {elapsed_time2/10**9} s')
        
        return self._get_best()
            
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        print(f'Start {self._id}')
        if not self.canSchedule(cluster, topology):
            return None
        
        # The number of food has be found by an ant.
        best = self._meta_algorithm(cluster, topology)

        if best == None:
            return None
        
        return best
