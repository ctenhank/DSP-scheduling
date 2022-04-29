from typing import Dict, List
from dsp_simulation.cluster.physical_node import PhysicalNode


class Network:
    INTER_THREAD = 1
    INTER_PROCESS = 4
    INTER_NODE = 9
    INTER_RACK = 16


class Objective:
    TYPE = ['NETWORK_DISTANCE', 'AVAILABILITY']
    @classmethod
    def objectvie_weighted_sum(cls, assignment: List[PhysicalNode], weight_network=0.5, weight_failure=0.5):
        return weight_network * Objective.topology_network_distance(assignment) + weight_failure * Objective.system_failure(assignment) 
    
    #@classmethod
    #def objectvie_weighted_sum(cls, assignment: List[PhysicalNode], weight_network=1):
    #    return weight_network * Objective.topology_network_distance(assignment)
    
    #@classmethod
    #def objectvie_weighted_sum(cls, assignment: List[PhysicalNode], weight_failure=0.5):
    #    return weight_failure * Objective.system_failure(assignment) 
    
    @classmethod
    def resource_balanced(cls):
        pass
    
    @classmethod
    def topology_network_distance(cls, assignment: List[PhysicalNode]):
        network_dist = 0
        
        len_assignment = len(assignment)
        for i in range(len_assignment):
            for j in range(i+1, len_assignment):
                network_dist += get_network_distance(assignment[i], assignment[j])
                
        return network_dist
    
    @classmethod
    def system_failure(cls, assignment: List[PhysicalNode]):
        count:Dict[PhysicalNode, int] = {}
        for node in assignment:
            if node not in count:
                count[node] = 0
            count[node] += 1
        
        failure = {}
        for node in count:
            failure[node] = (100 - node.availability) * count[node]
        
        ret = 1.0
        for key  in failure:
            ret *= failure[key]
        #print(ret)
            
        return ret
    
def get_network_distance(pn1: PhysicalNode, pn2: PhysicalNode):
    """Get a distance from a worker and other worke.
    In this version, we only implemented using network distance.
    But, We should add other metrics for multi-objective optimization.

    Args:
        pn1 (PhysicalNode): Source Physical Node
        pn2 (PhysicalNode): Target Physical Node

    Returns:
        Network Distance: _description_
    """
    ret = 1
    if pn1.id == pn2.id:
        ret = Network.INTER_PROCESS
    elif pn1.rack != pn2.rack:
        ret = Network.INTER_RACK   
    else:
        ret = Network.INTER_NODE
    return ret