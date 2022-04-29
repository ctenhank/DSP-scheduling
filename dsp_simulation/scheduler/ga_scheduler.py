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
    
    def _select_randomly_node(self, nodes, node_info):
        node = rd.choice(nodes)
        while node_info[node.id] <= 0:
            node = rd.choice(nodes)
        return node
        
    def _initialize_individual(self) -> List[PhysicalNode]:
        """Select randomly the nodes of cluster to allocate the topology

        Returns:
            List[PhysicalNode]: _description_
        """
        
        
        available_nodes = self._cluster.get_available_physical_node()
        
        ret = []
        node_info = {}
        for node in available_nodes:
            node_info[node.id] = node.available_worker_cnt
        
        len_subgraph = len(self._topology.taskgraph.subgraph)
        for _ in range(len_subgraph):
            node = self._select_randomly_node(available_nodes, node_info)
            node_info[node.id] -= 1
            ret.append(node)
        #print(ret)
        return ret
        
    def offstring_update(self):
        pass




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
        super().__init__(__class__.__name__)
        self._num_iteration = num_iter
        self._num_generation = 0
        self._num_population = num_pop
        self._num_crossover = num_cross
        self._num_mutation = num_mut
        self._best_so_far = sys.maxsize
        
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
        # required worker number
        info = {}
        avail_info = {}
        for node in assignment:
            if node.id not in info:
                info[node.id] = 0
            info[node.id] += 1
            
        for node in cluster.nodes:
            if node.id in info:
                #if node not in avail_info:
                    #avail_info[node] = node.available_worker_cnt
                if node.available_worker_cnt < info[node.id]:
                    return False
        return True
                
    
    # Can I impove the time?
    def _meta_algorithm(self, cluster: Cluster, topology: Topology) -> List[PhysicalNode]:
        best_idx = -1
        num_top = int(self._num_population * 0.9)
        total_time_fitness = 0.0
        total_time_crossover = 0.0
        total_time_mutation = 0.0
        total_time_sorting = 0.0
        ga_st = datetime.now()
        outdir = Path('logs/ga')
        outdir.mkdir(exist_ok=True, parents=True)
        file = open(f'{str(outdir)}/elapsed-time-{ga_st}.log', 'w')
        file2 = open(f'{str(outdir)}/performance-{ga_st}.log', 'w')
        print(f'num_pop,nim_cross,num_mut: {self._num_population},{self._num_crossover},{self._num_mutation}', file=file)

        #population = [ Individual(topology, cluster) for _ in range(self._num_population)]
        population = []
        while len(population) < 300:
            ind = Individual(topology, cluster)
            if self._check_available_case(cluster, ind.assignment):
                population.append(ind)
        
        while self._num_iteration >= self._num_generation:      
            scores = []
            d_scores = {}
            stime = time.time()
            for i in range(self._num_population):
                score = Objective.objectvie_weighted_sum(population[i].assignment)
                scores.append(score)
                d_scores[i] = score
                #print(score)
                
                # 
                if self._best_so_far > score:
                    #print(f'Generation {self._num_generation}: {self._best_so_far}')
                    self._best_so_far = score
                    best_idx = i
                
            print(f'Generation {self._num_generation}: {self._best_so_far}', file=file2, flush=True)
            #print(f'Generation {self._num_generation}: {self._best_so_far}')
            
            time_fitness = time.time() - stime
            total_time_fitness += time_fitness
            
            # Crossover
            stime = time.time()
            for _ in range(self._num_crossover):
                idx1, idx2 = self._tourmament_selection(scores)
                parent1, parent2= population[idx1], population[idx2]
                child= deepcopy(parent1)
                
                pivot = rd.randint(0, min(len(parent1.assignment), len(parent2.assignment)) - 1)
                # check here
                child.assignment = parent1.assignment[:pivot] + parent2.assignment[pivot:]
                if len(topology.taskgraph.subgraph) != len(child.assignment):
                    continue
                
                if not self._check_available_case(cluster, child.assignment):
                    continue
                
                if child not in population:
                    #score_child = Fitness.fitness_weighted_sum(child)
                    population.append(child)
                    #scores.append(score_child)
                    d_scores[len(population) - 1] = Objective.objectvie_weighted_sum(child.assignment)
            time_crossover = time.time() - stime
            total_time_crossover += time_crossover
            
            
            # Mutation
            stime = time.time()
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
                
                    
                #idx1, idx2 = rd.choices([i for i in range(len(mutant.assignment))], k=2)
                #temp = mutant.assignment[idx1]
                #mutant.assignment[idx1] = mutant.assignment[idx2]
                #mutant.assignment[idx2] = temp
                
                if mutant not in population:
                    #score_mutant = Fitness.fitness_weighted_sum(mutant)                                
                    population.append(mutant)
                    #scores.append(score_mutant)
                    d_scores[len(population) - 1] = Objective.objectvie_weighted_sum(mutant.assignment)                                
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
        
        #print(f'Avg fitness time: {total_time_fitness / self._num_iteration}')
        #print(f'Avg crossover time: {total_time_crossover / self._num_iteration}')
        #print(f'Avg mutation time: {total_time_mutation / self._num_iteration}')
        #print(f'Avg sorting time: {total_time_sorting / self._num_iteration}')
        file.close()
        file2.close()
        
        return population[best_idx].assignment
    
    def schedule(self, cluster: Cluster, topology: Topology) -> bool:
        if not self.canSchedule(cluster, topology):
            return None
        
        best = self._meta_algorithm(cluster, topology)
        if best == None:
            return False
        
        #temp = self._check_available_case(cluster, best)
        #print(temp)
        #print(len(topology.taskgraph.subgraph))
        #print(len(best))
        ##if self._check_available_case(cluster, best):
        ##    print('True')
        #    #print('True')
        #    #print('True')
        #    #print('True')
        #    #print('True')
        ##print(best)
        ##print(best)
        ##print(best)
        #for t in best:
        #    print(t.id, end=' ')
        #print()
        cluster.assign_topology(topology, best)
        return True
