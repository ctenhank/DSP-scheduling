from dsp_simulation.topology.vertex import Vertex
import random as rd

def global_grouping(source:Vertex, target: Vertex):
        edges = []
        for task in source.tasks:
            edges.append((task, target.tasks[0]))
        return edges       
                
    
def shuffle_grouping(source: Vertex, target: Vertex):
    """
    There must be an edge for each 'to' whatever the number of from_ and to.
    
    Randomly make a edge between from and to.
    In the case, the number of from is more larger than to, 
    """
    
    visited_source = [False for _ in range(len(source.tasks))]
    visited_target = [False for _ in range(len(target.tasks))]
    
    edges = []
    
    if len(source.tasks) <= len(target.tasks):
        for idx, task in enumerate(source.tasks):
            if visited_source[idx]:
                continue
            
            visited_source[idx] = True
            target_idx = rd.randint(0,len(target.tasks)-1)
            while visited_target[target_idx]:
                target_idx = rd.randint(0,len(target.tasks)-1)
            visited_target[target_idx] = True
            edges.append((source.tasks[idx], target.tasks[target_idx]))    
        
            
        for idx, task in enumerate(target.tasks):
            if visited_target[idx]:
                continue
            visited_target[idx] = True
            source_idx = rd.randint(0,len(source.tasks)-1)
            edges.append((source.tasks[source_idx], target.tasks[idx]))   
            
    else:
        for idx, task in enumerate(target.tasks):
            if visited_target[idx]:
                continue
            
            visited_target[idx] = True
            source_idx = rd.randint(0, len(source.tasks) -1)
            while visited_source[source_idx]:
                source_idx = rd.randint(0, len(source.tasks) -1)
            visited_source[source_idx] = True
            edges.append((source.tasks[source_idx], target.tasks[idx]))    
            
        for idx, task in enumerate(source.tasks):
            if visited_source[idx]:
                continue
            
            target_idx = rd.randint(0, len(target.tasks)-1)
            edges.append((source.tasks[idx], target.tasks[target_idx]))
                        
        #for idx, source_task in enumerate(source.tasks):
        #    if visited_source[idx]:
        #        continue
            
        #    visited_source[idx] = True
        #    target_idx = rd.randint(0,len(target.tasks)-1)
        #    edges.append(source_task, target.tasks[id])
    return edges