from typing import Deque, Dict
from dsp_simulation.etc.clock import SystemClock

from dsp_simulation.etc.message import Message
from dsp_simulation.simulator.generator import GaussianGenerator
from dsp_simulation.topology.task import Task
from collections import deque

class Packet:
    def __init__(self, src: Task, dest: Task, msg: Message):
        self.src = src
        self.dest = dest
        self.msg = msg

class Network:
    """The mean and 
    """
    
    # unit: ms
    THREAD_MEAN = 0.004380266651921919 * 5
    PROCESS_MEAN = 0.019515183 * 5
    NODE_MEAN = 0.17600326 * 5
    RACK_MEAN = NODE_MEAN * 2
    DCN_MEAN = 5.7412367 
    THREAD_STD = 0.0006388322379872011
    PROCESS_STD = 0.004303672
    NODE_STD = 0.05225135
    RACK_STD = NODE_STD * 2
    DCN_STD = 0.10867026
    
    TYPE = ['INTER-THREADS', 'INTER-PROCESS', 'INTER-NODE', 'INTER-RACK']
    DISTRIBUTION = {
        'INTER-THREADS' : GaussianGenerator(THREAD_MEAN, THREAD_STD),
        'INTER-PROCESS' : GaussianGenerator(PROCESS_MEAN, PROCESS_STD),
        'INTER-NODE' : GaussianGenerator(NODE_MEAN, NODE_STD),
        'INTER-RACK' : GaussianGenerator(RACK_MEAN, RACK_STD)
    }
    
    def __init__(self):
        self._queue: Dict[float, Deque[Packet]] = {}
        self._cnt = 0
    
    def route(self, src:Task, dest: Task, msg: Message):
        if msg.rcv_time not in self._queue:
            self._queue[msg.rcv_time] = [Packet(src, dest, msg)]
        else:
            self._queue[msg.rcv_time].append(Packet(src, dest, msg))
            
    def complete(self):
        past = []
        for key in self._queue:
            if key <= SystemClock.CURRENT:
                #print(f'{key}: {self._queue[key]}')
                while self._queue[key]:
                    pkt = self._queue[key].pop()
                    self._cnt += 1
                #for pkt in self._queue[key]:
                    pkt.dest.receive(pkt.src.vertex_id, pkt.msg)        
                past.append(key)
        for key in past:
            del self._queue[key]
    
    def send(cls, source: Task, target: Task, rcv_time: float):     
        pass
   
    def check_communication_type(self, source: Task, target: Task):
        pass
        