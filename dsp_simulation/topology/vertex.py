import uuid
from typing import List
import random as rd

import numpy
from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator

from dsp_simulation.topology.task import Task


class Vertex:
    TYPE = ['source', 'operator']

    def __init__(self, capability=10, parallelism=3, type=None, name=None):
        if name is None:
            self.__id = 'vertex-' + str(uuid.uuid1())
        else:
            self.__id = name

        self.__type = type
        self.__parallelism = parallelism
        self.__cap = capability

        # physical node의 리소스와 현재 태스크를 수행하기 위해 필요한 컴퓨팅 리소스를 비교해서 latency simulator를 실행시킴
        self.__tasks: List[Task] = self._mk_task()

    def __str__(self):
        ret = '-' * 50 + '\n'
        #ret += 'Vertex Info: id, capability, type\n'
        ret += f'Vertex: {self.__id}, {self.__cap}, {self.__type}, {self.__parallelism}\n'
        ret += '-' * 50 + '\n'
        ret += '  Task Info: id, latency_generator, iterations\n'
        for task in self.__tasks:
            ret += str(task) + '\n'
        ret += '-' * 50
        return ret

    @property
    def id(self):
        return self.__id

    @property
    def type(self):
        return self.__type

    @property
    def parallelism(self):
        return self.__parallelism

    @property
    def tasks(self):
        return self.__tasks

    @property
    def capability(self):
        return self.__cap

    def _mk_task(self):
        tasks = []
        cnt = 0
        for _ in range(self.__parallelism):
            tasks.append(
                Task(
                    required_cap=self.__cap,
                    model=GaussianLatencyGenerator(jitter_model=numpy.random.standard_normal),
                    name=self.__id + '-'+str(cnt)
                )
            )
            cnt += 1
        return tasks
