import random as rd
import uuid
from components.physical_node import PhysicalNode

#from components.worker import Worker


class Cluster:
    RANDOM_KEY = ['max_node', 'max_worker',
                  'max_rack', 'max_cpu', 'max_memory']

    def __init__(self, random: bool, **kwargs):
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

        self.__racks = []
        self.__nodes = []
        self.__workers = []

        if random:
            max_node = 6
            max_worker = 2
            max_rack = 3
            max_cpu = 1000.0 * 8
            max_memory = 10000.0 * 8

            for key, value in kwargs.items():
                if key in Cluster.RANDOM_KEY:
                    if key == Cluster.RANDOM_KEY[0]:
                        max_node = value
                    elif key == Cluster.RANDOM_KEY[1]:
                        max_worker = value
                    elif key == Cluster.RANDOM_KEY[2]:
                        max_rack = value
                    elif key == Cluster.RANDOM_KEY[3]:
                        max_cpu = value
                    elif key == Cluster.RANDOM_KEY[4]:
                        max_memory = value

            self._generate_random_cluster(
                max_node=max_node,
                max_worker=max_worker,
                max_rack=max_rack,
                max_cpu=max_cpu,
                max_memory=max_memory
            )
        else:
            pass
            # TODO: generate sample cluster
            # self._generate_sample_cluster(config)

    def __str__(self):
        ret = ''
        ret += '='*50 + '\n'
        ret += f'Racks Info: {self.__racks}\n'
        ret += '-'*50 + '\n'
        ret += f'Nodes Info: id/cpu/memory/rack/remain_cpu/remain_memory/num_worker\n'
        for node in self.__nodes:
            ret += str(node) + '\n'
        ret += '-'*50 + '\n'
        ret += f'Workers Info:\n'
        for worker in self.__workers:
            ret += str(worker) + '\n'
        ret += '='*50 + '\n'
        return ret

    def _generate_sample_cluster(self, config):
        pass

    def _generate_random_cluster(self, max_node, max_worker, max_rack, max_cpu, max_memory):
        """Generate a random cluster.

        Args:
            max_node (int): The max number of node
            max_worker (int): The max number of worker
            max_rack (int): The max number of rack
            max_cpu (int): The max available cpu
            max_memory (int): The max available memory
        """
        self.__racks = self._generate_random_racks(max_rack)
        self.__nodes = self._generate_random_nodes(
            max_node=max_node,
            max_worker=max_worker,
            max_cpu=max_cpu,
            max_memory=max_memory
        )

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

    def _generate_random_nodes(self, max_node, max_worker, max_cpu, max_memory):
        """Generate a random physical nodes.

        Args:
            max_worker (int): The max number of worker
            max_cpu (int): The max available cpu
            max_memory (int): The max available memory

        Returns:
            list[PhysicalNode]: _description_
        """
        # 현 상황에서 봤을 때는 Worker가 필요없는 것 같다
        available_cpu = rd.randint(max_cpu / 2, max_cpu)
        available_memory = rd.randint(max_memory/2, max_memory)
        num_node = rd.randint(int(max_node/2), max_node)

        nodes = []
        while available_cpu > 0 and available_memory > 0 and num_node > 0:
            rack = rd.choice(self.__racks)
            num_worker = rd.randint(int(max_worker/2), max_worker)
            assigned_cpu = rd.randint(1, available_cpu)
            assigned_memory = rd.randint(1, available_memory)
            node = PhysicalNode(
                assigned_cpu, assigned_memory, rack, num_worker)
            nodes.append(node)

            num_node -= 1
            available_cpu -= node.cpu_capability
            available_memory -= node.memory_capability
            num_worker -= node.num_worker

        return nodes
