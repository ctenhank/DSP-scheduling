import random as rd
from typing import List
import uuid
from component.physical_node import PhysicalNode

#from components.worker import Worker


class Cluster:
    RANDOM_KEY = ['max_node', 'max_worker',
                  'max_rack', 'max_cap']

    def __init__(self, random: bool, max_node=6, max_rack=3, max_worker=3):
        """The class generates a virutual cluster to simulate the scheduling methods. 
        There are two methods to create a cluster; one is randomly generation, and the other is used a pre-defined configuration.
        The default method is random-based generator, but if you want to use other method, you should set random arguemnt as False.
        
        For simplicity, we assume there is no master node just replaced to scheduling algorithm.
        
        And you can set some arugments related to cluster creation; the max number of rack, workers, slots for each worker, cpu, and memory.
        Each keywords set as kwargs is like this: ['max_node', 'max_worker_process', 'max_rack', 'max_cpu', max_memory'].
        The default values for each keyword are 6, 2, 3, 1000.0 * 8, 10000.0 * 8 refered by the jerry peng of project to compare his scheduler and others.
        The unit of CPU is usage percent per a core(0 to 100.0) and unit of memory is MB(0.0 ~ INF).
        The link is [here](https://github.com/jerrypeng/storm-scheduler-test-framework).
        
        In this version, we only support the random-based generator.

        Args:
            random (bool): The flag whether random-based cluster generator or pre-defined configuration-based generator
        """
        
        if max_node <= 3:
            print(f'The maximum number of nodes should be over and equal than 3. Current value is {max_node}')
            exit(1)

        self.racks= []
        self.nodes: List[PhysicalNode] = []

        if random:
            self.racks = self._generate_random_racks(max_rack)
            self.nodes = self._generate_random_nodes(max_node, max_worker)
        else:
            # TODO: generate sample cluster
            # self._generate_sample_cluster(config)
            pass
            

    def __str__(self):
        ret = '=' * 25 + 'Cluster Info' + '='* 25 + '\n'
        ret += f'Racks Info: {self.racks}\n'
        ret += '-'*50 + '\n'
        ret += f'Nodes Info: node_id/capability/rack_type/remain_capability\n'
        for node in self.nodes:
            ret += str(node) + '\n'
        ret += '-'*50 + '\n'
        #ret += f'Workers Info:\n'
        #for worker in self.__workers:
        #    ret += str(worker) + '\n'
        #ret += '='*50 + '\n'
        ret += '='*50 + '\n'
        return ret

    def _generate_sample_cluster(self, config):
        pass

    def _generate_random_racks(self, max_rack: int):
        """Generate random rack.
        The rack means that a frame can be mounted multiple computing resource(usually server).
        So, it can be used as simple network metrics such like physical distance between worker.

        Args:
            max_rack (int): the max number of rack

        Returns:
            list[str]: the list of rack's id
        """
        num_rack = rd.randint(1, max_rack)
        list_rack = []
        for _ in range(num_rack):
            list_rack.append('rack-' + str(uuid.uuid1()))
        return list_rack

    def _generate_random_nodes(self, max_node, max_worker):
        """Generate a random physical nodes.

        Args:
            max_node (int): The max number of Physical Node
        Returns:
            list[PhysicalNode]: The list of randomly created nodes
        """
        num_node = rd.randint(int(max_node/2), max_node)

        nodes = []
        while num_node > 0:
            rack = rd.choice(self.racks)
            num_worker = rd.randint(1, max_worker)
            node = PhysicalNode(rack, num_worker)
            nodes.append(node)
            num_node -= 1

        return nodes
    
    def get_available_physical_node(self) -> List[PhysicalNode]:
        """Get available physical machines in this cluster.

        Returns:
            List[PhysicalNode]: The list of physical node which has available worker
        """
        ret = []
        for node in self.nodes:
            worker = node.get_available_worker()
            if len(worker) > 0:
                ret.append(node)
        return node