from abc import ABCMeta, abstractmethod
import pickle
import time
from typing import Deque, Dict, List
import numpy as np
import uuid
from collections import deque
from dsp_simulation.etc.clock import SystemClock

from dsp_simulation.etc.message import Message
from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator, LatencyGenerator

from pathlib import Path
import datetime

tz = datetime.timezone(datetime.timedelta(hours=9))
now = datetime.datetime.now(tz)
outdir = Path(f'./log/{now}')

class Task(metaclass=ABCMeta):
    def __init__(self, vertex_id, name=None):
        """_summary_

        Args:
            vertex_id (_type_): Id of task's parent, Vertex.
            name (_type_, optional): task_name

        Returns:
            _type_: _description_
        """
        
        if name is not None:
            self._id = name
        else:
            self._id = 'task-' + str(uuid.uuid1())
        self._vertex_id = vertex_id
    
    def __str__(self):
        return self._id
    
    @abstractmethod
    def start(self):
        pass     
    
    @abstractmethod
    def _post_result(self):
        pass
    
    @abstractmethod
    def receive(self, source: str, msg: Message):
        pass
    
    @abstractmethod
    def shutdown(self):
        pass
    
    @property
    def id(self):
        return self._id
    
    @property
    def vertex_id(self):
        return self._vertex_id
    

class SourceTask(Task):
    def __init__(self, vertex_id, drate, name=None, dsize_mean=250, dsize_std=25):
        super().__init__(vertex_id, name)
        self._data_rate = drate
        self._time_for_data = 1 / self._data_rate
        self._data_size_gernerator = GaussianLatencyGenerator(dsize_mean, dsize_std)
        self._data_size = []
        self._snd_msg_cnt = 0
        self.share = 0
    
    def update_datarate(self, drate):
        self._data_rate = drate
    
    def _post_result(self):
        pass
    
    def receive(self, source: str, msg: Message):
        pass
    
    def shutdown(self):
        global outdir
        basedir: Path = outdir / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir/ (self._id + '.pkl')
    
        obj = {
            'data_rate': self._data_rate,
            'data_size': self._data_size,
            'sent_message': self._snd_msg_cnt
        }
        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)
        print(f'Writing a log file to {filepath}')
        
    def start(self):
        current = int(SystemClock.CURRENT / self._time_for_data)
        #print(current, self.share)
        if current > self.share:
            msg = []
            for _ in range(current - self.share):
                msg_size = self._data_size_gernerator.next_latency_ms()
                msg.append(Message(
                event_time=SystemClock.CURRENT,
                msg_size=msg_size,
                vertex_id=self.vertex_id
                ))
                self._data_size.append(msg_size)
            
            self.share = current

            return {
                'msg': msg
            }
    

# 더이상 건드릴것없음
class SinkTask(Task):
    def __init__(self, vertex_id, indegree: List[str], name=None):
        super().__init__(vertex_id, name)
        self._rcv_msg_cnt = 0
        self._msg_cnt_2s = 0
        self._rcv_msg_cnt = {}
        self._end_to_end_delay: List[float] = []
        #self._queue:Dict[str, Deque[Message]] = {}
        self._queue:Deque[Message] = []

        if indegree is not None:
            for indegree_id in indegree:
                self._rcv_msg_cnt[indegree_id] = 0
                
    def _post_result(self):
        #self._throughput += 1
        #self._measure_time += self._execute_latency[-1]
        
        q, r = divmod(int(SystemClock.CURRENT), 2)
        if q != self._interval_time and r == 0:
            self._interval_time = q
            print(f'{self._id} ({int(SystemClock.CURRENT)}): throughput({self._msg_cnt_2s})')
            self._msg_cnt_2s = 0
            
                      
    def start(self):
        
        #stime = time.time()
        while self._queue:
            e = self._queue.pop()
            self._end_to_end_delay.append(e.accumulated_latency)
            self._msg_cnt_2s += 1
            #print(f'Response Time(ms): {e.accumulated_latency}')
    
        return None

    
    def shutdown(self):
        global outdir
        basedir: Path = outdir / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir/ (self._id + '.pkl')
        
        obj = {
            'received_message': self._rcv_msg_cnt,
            'e2e_delay': self._end_to_end_delay
        }
        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)
        print(f'Writing a log file to {filepath}')
    
    def receive(self, source: str, msg: Message):
        self._queue.append(msg)
        if source not in self._rcv_msg_cnt:
            self._rcv_msg_cnt[source] = 1
        elif source in self._rcv_msg_cnt:
            self._rcv_msg_cnt[source] += 1
    

class OperatorTask(Task):
    def __init__(self, vertex_id, selectivity, productivity, indegree: List[str]=None, name=None, cap=None):
        """_summary_

        Args:
            vertex_id (_type_): _description_
            sel (_type_): Selectivity
            prod (_type_): Productivity
            name (_type_, optional): _description_. Defaults to None.
        """
        super().__init__(vertex_id, name)
        self._selectivity = selectivity
        self._productivity = productivity
        self._latency_generator: LatencyGenerator = None
        self._assigned_cap = cap
        self._throughput = 0
        self._throughput_2s = []
        self._processing_latency = []
        self._execute_latency = []
        self._snd_msg_cnt = 0
        self._rcv_msg_cnt = {}
        self._queue:Dict[str, Deque[Message]] = {}
        self._measure_time = 0.0
        self._interval_time = 0

        if indegree is not None:
            for indegree_id in indegree:
                self._queue[indegree_id] = deque()
                self._rcv_msg_cnt[indegree_id] = 0
                
    @property
    def assigned_cap(self):
        return self._assigned_cap
    
    @assigned_cap.setter
    def assigned_cap(self, cap):
        if cap < 0:
            print(f'The capability must be over than 0.0: {cap}')
            exit(1)
            
        self._assigned_cap = cap
        
    @property
    def throughput(self):
        ret = 0
        for thr in self._throughput_2s:
            ret += thr
        return ret
                
    def update_latency_generator(self, model: LatencyGenerator=None):
        self._latency_generator: LatencyGenerator = model

        
    def shutdown(self):
        global outdir
        basedir: Path = outdir / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir/ (self._id + '.pkl')
        
        #throughput = [self._throughput_2s[0]]
        #for idx in range(len(self._throughput_2s[1:])):
        #    throughput.append(self._throughput_2s[idx] - self._throughput_2s[idx - 1])
        obj = {
            'throughput': self._throughput_2s,
            'execute_latency': self._execute_latency,
            'processing_latency': self._processing_latency,
            'productivity': self._productivity,
            'selectivity': self._selectivity,
            'capability': self._assigned_cap,
            'received_messasge': self._rcv_msg_cnt,
            'sent_message': self._snd_msg_cnt
        }
        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)
        print(f'Writing a log file to {filepath}')
        
    def _ready(self):
        """If there are various incoming data from multiple preceding operators, it should be blocked up to there is a data for each queue

        Returns:
            _type_: _description_
        """
        for key in self._queue:
            if len(self._queue[key]) < 1:
                return False
            
            can = self._queue[key][0].event_time + self._queue[key][0].transmission_delay
            if SystemClock.CURRENT < can:
                return False
        return True
    
    def _post_result(self):
        self._throughput += 1
        self._measure_time += self._execute_latency[-1]
        
        q, r = divmod(int(SystemClock.CURRENT), 2)
        if q != self._interval_time and r == 0:
            self._interval_time = q
            self._throughput_2s.append(self._throughput)
            print(f'{self._id} ({int(SystemClock.CURRENT)}): throughput({self._throughput})')
            self._throughput = 0
    
    def receive(self, source: str, msg: Message):
        if source in self._queue:
            self._queue[source].append(msg)
            self._rcv_msg_cnt[source] += 1
            
    def _pop_data(self)-> List[Message]:
        ret = []
        for key in self._queue:
            ret.append(self._queue[key].popleft())
        return ret
    
    def _processing(self):
        # 데이터 사이즈의 변화에 따라 레이턴시 변화도 있어야 하지 않을까? -> 고려하지 않음
        input:List[Message] = self._pop_data()
        latency = self._latency_generator.next_latency_ms()
        self._processing_latency.append(latency)
        
        size_output= 0
        num_output = len(input) * self._selectivity
        max_delay = -1.0
        for i in input:
            size_output += i.msg_size
            max_delay = max(max_delay, i.accumulated_latency)

        size_output *= self._productivity
        msg = [Message(SystemClock.CURRENT, size_output, self._vertex_id, max_delay) for _ in range(num_output)]
        return msg
        
    def start(self):
        """_summary_
        """
        
        if self._ready():
            stime = time.time()
            res = self._processing() 
        
        
            execute_latency = self._processing_latency[-1] + (time.time() - stime)
            self._execute_latency.append(execute_latency)
            self._post_result()
            
            for msg in res:
                msg.update_accumulated_latency(self._execute_latency[-1])
            
            return {
                'msg': res,
                'execute_latency': self._execute_latency[-1],
                'processing_latency': self._processing_latency[-1]
            }
        return None