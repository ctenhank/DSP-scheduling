import random as rd
from typing import List, Mapping, Tuple
from dsp_simulation.cluster.physical_node import PhysicalNode
#from dsp_simulation.topology.subtopology import SubTopology
from dsp_simulation.topology.topology import Topology

class Cluster:
    RANDOM_KEY = ['max_node', 'max_worker',
                    'max_rack', 'max_cap']
    RACK_PER_NODES = 8
    RACK_CNT = 0

    def __init__(self, random: bool, max_node: int=50, max_rack: int=8, max_worker: int=10, num_physical_nodes_per_rack: int=8):
        """The class generates a virutual cluster to simulate the scheduling methods. 
        There are two methods to create a cluster; one is randomly generation, and the other is used a pre-defined configuration.
        The default method is random-based generator, but if you want to use other method, you should set random arguement as False. 
        In this version, we only support the random-based generator.
            
        For simplicity, we assume there is no master node just replaced to scheduling algorithm, and the resources affected the performance are only cpu and network.
            
        [Information]
        The Reference link: https://www.quora.com/How-many-servers-does-a-typical-data-center-house
        There is no the "typical" datacenter, but every standard rack may hold about 30-35 servers as a unit.
        The modern datacenters can have a few thousands of rack. In the micro-datacenter, it can have a few hundreds of rack.
        In the casual cases a kind of university lab, they can have a few racks(maybe 2-5), each racks has 5-10 servers.
        You can set the maximum number of servers and racks depending on your case.
            
        And you can set some arugments related to cluster creation; the max number of rack, workers, slots for each worker, computing capability.
        The unit of CPU is a performance percent per the core(0 to 100.0) based on the CPU, Intel(R) Core(TM) i7-8700K CPU @ 3.70GHz.

        Args:
            random (bool): The flag whether random-based cluster generator or pre-defined configuration-based generator
            max_node (int, optional): the maximum number of physical computing nodes.
            max_rack (int, optional): the maximum number of racks holding a few number of nodes as a unit.
            max_worker (int, optional): the maximum number of workers running a subgraph of a topology in a physical node.
            num_physical_nodes_per_rack (int, optional): the number of physical nodes per a rack. The default is to 8.
        """
            
        if max_node <= 3:
            print(f'The maximum number of nodes should be over and equal than 3. Current value is {max_node}')
            exit(1)

        self._racks= []
        self._nodes: List[PhysicalNode] = []
        #    self.__topologies: List[Topology] = []
        self._topology: List[Topology] = []
        self._assigned: dict = {}

        if random:
            self._nodes = self._generate_random_nodes(max_node, max_worker)
            self._racks = self._generate_random_racks(max_rack, num_physical_nodes_per_rack)
        else:
            # TODO: generate sample cluster
            # self._generate_sample_cluster(config)
            pass
            
        self._assign_rack_to_nodes()
                

    def __str__(self):
        ret = '=' * 25 + 'Cluster Info' + '='* 25 + '\n'
        ret += f'Racks Info: {self._racks}\n'
        ret += '-'*50 + '\n'
        ret += f'Nodes Info: node_id/capability/rack_type/remain_capability\n'
        for node in self._nodes:
            ret += str(node) + '\n'
        ret += '-'*50 + '\n'
        ret += '='*50 + '\n'
        return ret
        
    @property
    def racks(self):
        return self._racks
        
    @property
    def nodes(self):
        return self._nodes
        
    @property
    def topology(self):
        return self._topology

    def _generate_sample_cluster(self, config):
        pass

    def _generate_random_racks(self, max_rack: int, num_physical_nodes_per_rack: int):
        """Generate random rack.
        The rack means that a frame can be mounted multiple computing resource(usually server).
        So, it can be used as simple network metrics such like physical distance between worker.
            
        Args:
            max_rack (int): the max number of rack

        Returns:
            list[str]: the list of rack's id
        """
        num_rack = rd.randint(int(len(self._nodes) / num_physical_nodes_per_rack), max_rack)
        list_rack = []
            
        for _ in range(num_rack):
            list_rack.append('rack-' + str(Cluster.RACK_CNT))
            #list_rack.append('rack-' + str(uuid.uuid1()))
            Cluster.RACK_CNT += 1
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
            #rack = rd.choice(self.__racks)
            #node = PhysicalNode(rack, num_worker)
            node = PhysicalNode(max_worker)
            nodes.append(node)
            num_node -= 1

        return nodes
        
    def _assign_rack_to_nodes(self):
        """Assign randomly rack type to nodes
        """
        for node in self._nodes:
            rack = rd.choice(self._racks)
            node.rack = rack
    
    def get_available_physical_node(self) -> List[PhysicalNode]:
        """Get available physical machines in this cluster.

        Returns:
            List[PhysicalNode]: The list of physical node which has available worker
        """
        ret = []
        #print(f'length of nodes: {len(self.__nodes)}')
        for node in self._nodes:
            worker = node.get_available_worker()
            #print(f'{node}')
            if len(worker) > 0:
                ret.append(node)

        return ret

    # def submit(self, topology: Topology) -> bool:
    #     """_summary_

    #     Args:
    #         job_graph (JobGraph): _description_

    #     Returns:
    #         bool: _description_
    #     """
    #     if not self.get_available_physical_node():
    #         print(f'ERROR: There is no enough worker to execute {topology}')
    #         return False
        
    #     self._executable_job_graph.append(topology)
    #     return True       

    
    def check_topology_can_be_allocated(self, topology: Topology):
        """Check whether the given topology can be allocated in this cluster

        Args:
            topology (Topology): The topology to execute

        Returns:
            bool: 
        """
        available_nodes = self.get_available_physical_node()
        workers = []
        for node in available_nodes:
            workers.extend(node.get_available_worker())
                
        if len(workers) >= len(topology.taskgraph.subgraph):
            return True
            
        print('There are no enough resource and workers to run the topology in this cluster.')
        return False
            
        
    def assign_topology(self, topology: Topology, assignment: List[PhysicalNode]):
        self._topology.append(topology)
        
        for idx, node in enumerate(assignment):
            target_node = None
            for nd in self.nodes:
                if nd.id == node.id:
                    target_node = nd
                    break
            #print(f'{node.id}: {topology.taskgraph.subgraph[idx]}')
            res = target_node.assign(topology, idx)