from datetime import datetime
from pathlib import Path
from typing import List
from copy import deepcopy
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
from dsp_simulation.scheduler.objective import Objective
#from dsp_simulation.scheduler.metahueristic.fitness import Fitness
from dsp_simulation.scheduler.scheduler import MetaHueristicScheduler
from dsp_simulation.topology.topology import Topology

import random as rd
import sys
import time
#from dsp_simulation.scheduler.ga_scheduler import Individual

class Individual:
    def __init__(self, topology: Topology, cluster: Cluster):
        self.assignment: List[PhysicalNode] = self._initialize_individual(cluster, topology)
        
    #@property
    #def assignment(self):
    #    return self._assignment
    
    def _select_randomly_node(self, nodes, node_info):
        node = rd.choice(nodes)
        while node_info[node.id] <= 0:
            node = rd.choice(nodes)
        return node
        
    def _initialize_individual(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        """Select randomly the nodes of cluster to allocate the topology

        Returns:
            List[PhysicalNode]: _description_
        """
        available_nodes = cluster.get_available_physical_node()
        
        ret = []
        node_info = {}
        for node in available_nodes:
            node_info[node.id] = len(node.worker)

        len_subgraph = len(topology.taskgraph.subgraph)
        for _ in range(len_subgraph):
            node = self._select_randomly_node(available_nodes, node_info)
            node_info[node.id] -= 1
            ret.append(node)
        return ret
        



class GAScheduler(MetaHueristicScheduler):   
    """Genetic Algorithm algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """
    def __init__(self, num_iter=100, num_pop=300, num_cross=100, num_mut=50):
        """_summary_

        Args:
            topology (Topology): _description_
            cluster (Cluster): _description_
            num_iter (int, optional): The maximum number of iteration. Defaults to 100.
            num_pop (int, optional): The number of individuals in population. Defaults to 1000.
            num_cross (int, optional): The number of crossover. Defaults to 100.
        """
        super().__init__(f'{__class__.__name__}_{str(num_iter)}_{str(num_pop)}_{str(num_cross)}_{str(num_mut)}')
        self._num_iteration = num_iter
        self._num_generation = 0
        self._num_population = num_pop
        self._num_crossover = num_cross
        self._num_mutation = num_mut
        self._best_so_far = sys.maxsize
        self._node_info = {}
        
    def _tourmament_selection(self, scores, k=5):
        parents = []
        while len(parents) < k + 1:
            idx = rd.randint(0, len(scores) - 1)
            if idx not in parents:
                parents.append(idx)
        
        pair = []
        for i in range(k):
            pair.append((parents[i], scores[parents[i]]))
        
        pair.sort(key=lambda t: t[1])
        return pair[0][0], pair[1][0]
    
    
    def _check_available_case(self, cluster: Cluster, assignment: List[PhysicalNode]):
        info = {}
        for node in assignment:
            if node.id not in info:
                info[node.id] = 0
            info[node.id] += 1
        
        for node in cluster.nodes:
            if node.id in info:
                if len(node.worker) < info[node.id]:
                    return False
        return True                
                
    # Can I impove the time?
    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        best_idx = -1
        self._num_generation = 0
        self._best_so_far = sys.maxsize
        
        population = []
        cnt = 0 
        while cnt < self._num_population:
            ind = Individual(topology, cluster)
            if self._check_available_case(cluster, ind.assignment):
                population.append(ind)
                cnt += 1
        
        while self._num_iteration >= self._num_generation:      
            scores = []
            d_scores = {}
            for i in range(self._num_population):
                score = Objective.objectvie_weighted_sum(population[i].assignment)
                scores.append(score)
                d_scores[i] = score
                
                if self._best_so_far > score:
                    self._best_so_far = score
            
            # Crossover
            for _ in range(self._num_crossover):
                idx1, idx2 = self._tourmament_selection(scores)
                parent1, parent2= population[idx1], population[idx2]
                child= deepcopy(parent1)
    
                pivot = rd.randint(0, min(len(parent1.assignment), len(parent2.assignment)) - 1)
                child.assignment = parent1.assignment[:pivot] + parent2.assignment[pivot:]
                
                if len(topology.taskgraph.subgraph) != len(child.assignment):
                    continue
                
                if not self._check_available_case(cluster, child.assignment):
                    continue
                
                if child not in population:
                    population.append(child)
                    d_scores[len(population) - 1] = Objective.objectvie_weighted_sum(child.assignment)
            
            
            # Mutation
            for _ in range(self._num_mutation):
                idx = rd.randint(0, len(population) - 1)                
                mutant = deepcopy(population[idx])
                
                # Swap
                num = len(topology.taskgraph.subgraph)
                rd_cnt = rd.randint(1, num)
                idx_arr = [i for i in range(num)]
                for _ in range(rd_cnt):
                    idx = rd.choice(idx_arr)
                    mutant.assignment[idx] = rd.choice(cluster.nodes)
                
                
                if len(topology.taskgraph.subgraph) != len(mutant.assignment):
                    continue
                
                if not self._check_available_case(cluster, mutant.assignment):
                    continue
                
                if mutant not in population:
                    population.append(mutant)
                    d_scores[len(population) - 1] = Objective.objectvie_weighted_sum(mutant.assignment)                                
            
            
            # Sorting
            sorted_tuples = sorted(d_scores.items(), key=lambda item: item[1])
            next_population = []
            cnt = 0
            
            for tuple in sorted_tuples:
                if cnt >= int(self._num_population * 0.8):
                    break
                    
                if self._check_available_case(cluster, population[tuple[0]].assignment):
                    next_population.append(population[tuple[0]])
                cnt += 1
            
            while len(next_population) < self._num_population:
                next_population.append(Individual(topology, cluster))
            
            population = next_population[:self._num_population]
            self._num_generation += 1 
    
        return population[0].assignment
    
    def schedule(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        if not self.canSchedule(cluster, topology):
            return None
        
        best = self._meta_algorithm(cluster, topology)
        
        if best == None:
            return None
        return best