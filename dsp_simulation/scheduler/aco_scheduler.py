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

class Ant:
    """An agent finds the best solution in the cluster.
    
    
    Implementation is refered from https://github.com/december0123/Pants
    Returns:
        _type_: _description_
    """
    CNT = 0 
    def __init__(self, starting: int, size: int):
        """Initialize an ant

        Args:
            current (int): starting node
            size (int): the size of cluster
        """
        self.current = starting
        self._id = 'Ant-' + str(Ant.CNT)
        self.visited = [self.current]
        self.unvisited = [i for i in range(size) if i != self.current]
        self.traveled = []
    
    def __str__(self):
        ret = f'{self._id}: '
        return ret


class ACOScheduler(MetaHueristicScheduler):
    """Ant Colony Optimization algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """

    def __init__(self, num_ants: int = 1000, alpha: float = 1.0, beta: float = 2.5, rho: float = 0.8, Q: float = 1.0, t0: float = 0.1):
        """_summary_

        Args:
            num_ant (_type_): _description_
            alpha (_type_): _description_
            beta (_type_): _description_
            rho (_type_): _description_
            Q (_type_): _description_
        """
        super().__init__(__class__.__name__)
        self._num_ants = num_ants
        self._alpha = alpha
        self._beta = beta
        self._rho = rho
        self._Q = Q
        self._tau: List[List[int]] = []
        self._eta: List[List[float]] = []
        self._ants: List[Ant] = []
        self._t0 = t0
        self._num_solution: int = 0
        self._num_available_workers: int = 0
        self._worker_matrix: dict = {}


    def _initialize_environment(self, cluster: Cluster, topology: Topology):
        """_summary_

        Args:
            topology (Topology): _description_
            cluster (Cluster): _description_
        """
        available_nodes = cluster.get_available_physical_node()
        available_matrix = {}
        num_available_workers = 0
        
        # Create a map (worker_idx = :class:PhysicalNode)
        prev, cur = 0, 0
        for node in available_nodes:
            num_available_workers += node.available_worker_cnt
            cur = num_available_workers
            for i in range(prev, cur):
                available_matrix[i] = node
            prev = cur
        self._num_available_workers = num_available_workers
        self._worker_matrix = available_matrix
        
        # Initialize the network matrix and pheromone matrix between available workers
        network_matrix = [[0 for _ in range(num_available_workers)] for _ in range(num_available_workers)]
        pheromone_matrix: List[List] = [[0.0 for _ in range(num_available_workers)] for _ in range(num_available_workers)]
        for i in range(num_available_workers):
            for j in range(i, num_available_workers):
                if i != j:
                    dist = get_network_distance(available_matrix[i], available_matrix[j])
                    network_matrix[i][j] = dist
                    network_matrix[j][i] = dist
                    pheromone_matrix[i][j] = self._t0
                    pheromone_matrix[j][i] = self._t0
        self._tau = pheromone_matrix
        self._eta = network_matrix
        
        # Set a starting point where an ant find to other solutions
        for i in range(self._num_ants):
            starting_point = rd.randint(0, num_available_workers - 1)
            self._ants.append(Ant(starting_point, num_available_workers))
            
        # The number of food has be found by an ant.
        self._num_solution = len(topology.taskgraph.subgraph)

    
    def _weight(self, cur: int, next: int) -> float:
        """Get a weight from current location to the next location by an ant

        Args:
            cur (int): current location
            next (int): next location
            
        Returns:
            float: the weight of current to next
        """
        
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
        
        available_nodes = cluster.get_available_physical_node()
        available_worker_cnt = {}
        
        for node in available_nodes: 
            available_worker_cnt[node.id] = node.available_worker_cnt
        
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
        best = sys.maxsize
        ret = []

        for ant in self._ants:
            assignment = []
            for worker_idx in ant.visited:
                assignment.append(self._worker_matrix[worker_idx])
                
            score = Objective.objectvie_weighted_sum(assignment)
            if best > score:
                best = score
                ret = assignment
                
        return ret
                

    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        self._initialize_environment(cluster, topology)
        while self._num_solution > 1:
            
            # the edges list from a to b
            movement: List[Tuple(int, int)] = []
            for ant in self._ants:
                movement.append(self._move_ant(ant))
            
            self._update_global_pheromone(movement)
                
            self._num_solution -= 1
        
        return self._get_best()
            
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        if not self.canSchedule(cluster, topology):
            return None
        best = self._meta_algorithm(cluster, topology)

        if best == None:
            return None
        
        #print(len(topology.taskgraph.subgraph), len(best))
        return best
        cluster.assign_topology(topology, best)
        return True
