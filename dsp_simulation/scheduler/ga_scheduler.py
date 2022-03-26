from datetime import datetime
from typing import List
from copy import deepcopy
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.cluster.physical_node import PhysicalNode
#from dsp_simulation.scheduler.metahueristic.fitness import Fitness
from dsp_simulation.scheduler.scheduler import MetaHueristicScheduler
from dsp_simulation.topology.topology import Topology

import sys
import random as rd
import time
#from dsp_simulation.scheduler.ga_scheduler import Individual





class Individual:
    def __init__(self, topology: Topology, cluster: Cluster):
        self._cluster = cluster
        self._topology = topology
        self.assignment: List[PhysicalNode] = self._initialize_individual()
        
    #@property
    #def assignment(self):
    #    return self._assignment
        
    def _initialize_individual(self) -> List[PhysicalNode]:
        """Select randomly the nodes of cluster to allocate the topology

        Returns:
            List[PhysicalNode]: _description_
        """
        
        
        available_nodes = self._cluster.get_available_physical_node()
        
        len_subgraph = len(self._topology.subgraph)
        #print(len_subgraph)
        ret = []
        node_info = {}
        for node in available_nodes:
            node_info[node.id] = node.available_worker_cnt
        
            #print(f'{node.id}: {node_info[node.id]}')
        #node_info[node.id] = node.available_worker_cnt
        
        # 여기서 막힘
        cnt = 0
        for _ in range(len_subgraph):
            cnt +=1
            node = rd.choice(available_nodes)
            # 정확하게는 여기서 블럭됨
            while node_info[node.id] <= 0:
                node = rd.choice(available_nodes)
                #print(node.id)
                
            node_info[node.id] -= 1
            ret.append(node)
        for node in ret:
            node_info[node.id] = node.available_worker_cnt
            #print(f'{node.id}: {node_info[node.id]}, {node.available_worker_cnt}')
        return ret
        
    def offstring_update(self):
        pass


class Network:
    INTER_THREAD = 1
    INTER_PROCESS = 4
    INTER_NODE = 9
    INTER_RACK = 16


class Fitness:
    @classmethod
    def fitness_weighted_sum(cls, assignment: List[PhysicalNode], weight_network=1):
        return weight_network * Fitness.topology_network_distance(assignment)
    
    @classmethod
    def resource_balanced(cls):
        pass
    
    @classmethod
    def topology_network_distance(cls, assignment: List[PhysicalNode]):
        network_dist = 0
        
        #assignment = ind.assignment
        len_assignment = len(assignment)
        for i in range(len_assignment):
            for j in range(i+1, len_assignment):
                if assignment[i].id == assignment[j].id:
                    network_dist += 4
                    #network_dist += Network.INTER_PROCESS
                elif assignment[i].rack != assignment[j].rack:
                    network_dist += 16
                    #network_dist += Network.INTER_RACK   
                else:
                    network_dist += 9
                    #network_dist += Network.INTER_NODE
                        
        return network_dist

class GAScheduler(MetaHueristicScheduler):   
    """Genetic Algorithm algorithm-based Scheduler

    Args:
        Scheduler (_type_): _description_
    """
    def __init__(self, topology: Topology, cluster: Cluster, num_iter=100, num_pop=300, num_cross=100, num_mut=50):
        """_summary_

        Args:
            topology (Topology): _description_
            cluster (Cluster): _description_
            num_iter (int, optional): The maximum number of iteration. Defaults to 100.
            num_pop (int, optional): The number of individuals in population. Defaults to 1000.
            num_cross (int, optional): The number of crossover. Defaults to 100.
        """
        super().__init__(num_iter)
        self._num_generation = 0
        self._num_population = num_pop
        self._num_crossover = num_cross
        self._num_mutation = num_mut
        self._best_so_far = sys.maxsize
        # Sometimes the running is blocked at here
        #self._population = [ Individual(topology, cluster) for _ in range(num_pop)]
        
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


    def _meta_algorithm(self, topology: Topology, cluster: Cluster) -> List[PhysicalNode]:
        best = -1
        num_top = int(self._num_population * 0.9)
        total_time_fitness = 0.0
        total_time_crossover = 0.0
        total_time_mutation = 0.0
        total_time_sorting = 0.0
        t = datetime.now()
        file = open(f'logs/elapsed-time-{t}.log', 'w')
        file2 = open(f'logs/performance-{t}.log', 'w')
        print(f'num_pop,nim_cross,num_mut: {self._num_population},{self._num_crossover},{self._num_mutation}', file=file)
        
        if not self.canSchedule(topology, cluster):
            return None

        population = [ Individual(topology, cluster) for _ in range(self._num_population)]
        
        while self._num_iteration >= self._num_generation:      
            scores = []
            d_scores = {}
            stime = time.time()
            for i in range(self._num_population):
                score = Fitness.fitness_weighted_sum(population[i].assignment)
                scores.append(score)
                d_scores[i] = score
                
                # 
                if self._best_so_far > score:
                    #print(f'Generation {self._num_generation}: {self._best_so_far}')
                    self._best_so_far = score
                    best = i
                
            print(f'Generation {self._num_generation}: {self._best_so_far}', file=file2, flush=True)
            
            time_fitness = time.time() - stime
            total_time_crossover += time_fitness
            
            # Crossover
            stime = time.time()
            for _ in range(self._num_crossover):
                idx1, idx2 = self._tourmament_selection(scores)
                parent1, parent2= population[idx1], population[idx2]
                child= deepcopy(parent1)
                
                pivot = rd.randint(0, min(len(parent1.assignment), len(parent2.assignment)) - 1)
                child.assignment = parent1.assignment[:pivot] + parent2.assignment[pivot:]
                
                if child not in population:
                    #score_child = Fitness.fitness_weighted_sum(child)
                    population.append(child)
                    #scores.append(score_child)
                    d_scores[len(population) - 1] = Fitness.fitness_weighted_sum(child.assignment)
            time_crossover = time.time() - stime
            total_time_crossover += time_crossover
            
            
            # Mutation
            stime = time.time()
            for _ in range(self._num_mutation):
                idx = rd.randint(0, len(population) - 1)                
                mutant = deepcopy(population[idx])
                
                # Swap
                idx1, idx2 = rd.choices([i for i in range(len(mutant.assignment))], k=2)
                temp = mutant.assignment[idx1]
                mutant.assignment[idx1] = mutant.assignment[idx2]
                mutant.assignment[idx2] = temp
                
                if mutant not in population:
                    #score_mutant = Fitness.fitness_weighted_sum(mutant)                                
                    population.append(mutant)
                    #scores.append(score_mutant)
                    d_scores[len(population) - 1] = Fitness.fitness_weighted_sum(mutant.assignment)                                
            time_mutation = time.time() - stime
            total_time_mutation += time_mutation
                    
            # Sorting
            stime = time.time()
            best = sorted(d_scores.items(), key=lambda item: item[1])[:num_top]
            next_population = []
            for t in best:
                next_population.append(population[t[0]])
            while len(next_population) < self._num_population:
                next_population.append(Individual(topology, cluster))
            population = next_population
            time_sorting = time.time() - stime
            total_time_sorting += time_sorting
            
            print(f'{time_fitness},{time_crossover},{time_mutation},{time_sorting}', file=file, flush=True)
            #print(f'  Elapsed fitness time: {time_fitness}')
            #print(f'  Elapsed crossover time: {time_crossover}')
            #print(f'  Elapsed mutation time: {time_mutation}')
            #print(f'  Elapsed sorting time: {time_sorting}')
            
            self._num_generation += 1 
        
        print(f'Avg fitness time: {total_time_fitness / self._num_iteration}')
        print(f'Avg crossover time: {total_time_crossover / self._num_iteration}')
        print(f'Avg mutation time: {total_time_mutation / self._num_iteration}')
        print(f'Avg sorting time: {total_time_sorting / self._num_iteration}')
        file.close()
        file2.close()
        
        return best      
    
    def schedule(self, topology: Topology, cluster: Cluster) -> bool:
        best = self._meta_algorithm(topology, cluster)
        if best == None:
            return False
        
        cluster.assign_topology(topology, best)
        return True
