import sys
import uuid
import time
import pickle
import datetime
import numpy as np

from abc import ABCMeta, abstractmethod
from typing import Deque, Dict, List
from pathlib import Path
from collections import deque
from dsp_simulation.etc.clock import SystemClock
from dsp_simulation.etc.message import Message
from dsp_simulation.simulator.generator import GaussianGenerator, Generator

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
    def post_result(self):
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
    def __init__(self, vertex_id, max_data_rate, name=None, dsize_mean=250, dsize_std=25, input_rate_dist=None):
        super().__init__(vertex_id, name)
        self._current_sec = int(SystemClock.CURRENT)
        self._max_data_rate = max_data_rate
        self._current_data_rate = int(input_rate_dist[self._current_sec] * self._max_data_rate)
        self._time_for_data = 1 / self._current_data_rate
        self._data_size_gernerator = GaussianGenerator(dsize_mean, dsize_std)
        self._data_size = []
        self._snd_msg_cnt = 0
        self._tot_snd_msg_cnt = 0
        self._last_executed = 0
        self._input_rate_dist = input_rate_dist
        self._round_start_time = 0
        self._current = 0
        self._previous_share = 0        
        
    @property
    def last_executed(self):
        return self._last_executed
    
    @property
    def current_sec(self):
        return self._current_sec
    
    @last_executed.setter
    def last_executed(self, current: SystemClock):
        if current < 0:
            return
        
        self._last_executed = int(current / self._time_for_data)
    
    def _update_data_rate(self):
        self._current_data_rate = int(self._max_data_rate * self._input_rate_dist[self._current_sec])
        self._time_for_data = 1 / self._current_data_rate
        
    
    def post_result(self):
        print(f'{self._id} ({int(SystemClock.CURRENT)}): Sent message count({self._snd_msg_cnt}), total sent message #({self._tot_snd_msg_cnt})')
        self._msg_cnt_2s = 0
        self._snd_msg_cnt = 0
        self._current_sec += 1
        self._last_executed = 0
        self._round_start_time = SystemClock.CURRENT
        
        self._update_data_rate()
    
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
        current = int((SystemClock.CURRENT % 1) / self._time_for_data) + 1
        
        if current > self._last_executed:
            msg_size = self._data_size_gernerator.next()
            self._data_size.append(msg_size)
            self._snd_msg_cnt += 1
            self._tot_snd_msg_cnt += 1
    
            self._last_executed = current

            return {
                'msg': Message(
                event_time=SystemClock.CURRENT,
                msg_size=msg_size,
                vertex_id=self.vertex_id
                )
            }
        
    
class SinkTask(Task):
    def __init__(self, vertex_id, indegree: List[str], name=None):
        super().__init__(vertex_id, name)
        self._rcv_msg_cnt = 0
        self._msg_cnt_2s = 0
        self._rcv_msg_cnt = {}
        self._end_to_end_delay: List[float] = []
        self._queue: Deque[Message] = deque()

        if indegree is not None:
            for indegree_id in indegree:
                self._rcv_msg_cnt[indegree_id] = 0
                
    def post_result(self):
        print(f'{self._id} ({int(SystemClock.CURRENT)}): Message count({self._msg_cnt_2s})')
        self._msg_cnt_2s = 0
            
                      
    def start(self):
        while self._queue:
            e = self._queue.pop()
            self._end_to_end_delay.append(e.accumulated_latency)
            self._msg_cnt_2s += 1
    
        return None

    
    def shutdown(self):
        global outdir
        basedir: Path = outdir / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir/ (self._id + '.pkl')
        print(f'sink e2e delay: {np.array(self._end_to_end_delay).mean()} ms')
        print(f'sink throughput: {self._rcv_msg_cnt}')
        
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
    def __init__(self, vertex_id, selectivity, productivity, indegree: List[str]=None, name=None, cap=None, latency_generator:Generator=None):
        """_summary_

        Args:
            vertex_id (_type_): _description_
            sel (_type_): Selectivity
            prod (_type_): Productivity
            name (_type_, optional): _description_. Defaults to None.
        """
        super().__init__(vertex_id, name)
        
        self._speed_up = None
        self._selectivity = selectivity
        self._productivity = productivity
        self._latency_generator = latency_generator
        
        
        self._throughput = []
        self._throughput_period = 0
        self._processing_latency = []
        self._processing_latency_period = []
        self._execute_latency = []
        self._execute_latency_period = []
        
        self._snd_msg_cnt = 0
        self._rcv_msg_cnt: Dict[str, int] = {}
        self._queue: Dict[str, Deque[Message]] = {}
        self._waiting_time:Dict[str, List] = {}
        self._arrival_time:Dict[str, List] = {}
        self._arrival_time_period: Dict[str, List] = {}
        
        self._executable_time = 0.0
    
        if indegree is not None:
            for indegree_id in indegree:
                self._queue[indegree_id] = deque()
                self._rcv_msg_cnt[indegree_id] = 0
                self._waiting_time[indegree_id] = []
                self._arrival_time[indegree_id] = []
                self._arrival_time_period[indegree_id] = []
                
    @property
    def speed_up(self):
        return self._speed_up
        
    @property
    def throughput(self):
        ret = 0
        for thr in self._throughput_2s:
            ret += thr
        return ret
                
    def update_speed_up(self, speed_up):
        self._speed_up = speed_up
        
    def shutdown(self):
        global outdir
        basedir: Path = outdir / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir/ (self._id + '.pkl')
    
        for key in self._waiting_time:
            print(f'Average waiting time({key}->{self.vertex_id}): {np.array(self._waiting_time[key]).mean()}ms')
        
        obj = {
            'throughput': self._throughput_2s,
            'execute_latency': self._execute_latency,
            'processing_latency': self._processing_latency,
            'productivity': self._productivity,
            'selectivity': self._selectivity,
            'capability': self._assigned_cap,
            'received_messasge': self._rcv_msg_cnt,
            'sent_message': self._snd_msg_cnt,
            'waiting_time': self._waiting_time[key]
        }
        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)
        print(f'Writing a log file to {filepath}')
        
    def _ready(self):
        """If there are various incoming data from multiple preceding operators, it should be blocked up to there is a data for each queue

        Returns:
            _type_: _description_
        """
        if self._executable_time > SystemClock.CURRENT:
            return False
        
        for key in self._queue:
            if len(self._queue[key]) < 1:
                return False
            if SystemClock.CURRENT < self._queue[key][0].rcv_time:
                return False

        return True
    
    def post_result(self):
        self._throughput.append(self._throughput_period)
        #print(f'{self._id} ({int(SystemClock.CURRENT)}): throughput({self._throughput})')
        
        arv_mean, arv_var = 0, 0
        keys = []
        for key in self._queue:
            keys.append(key)
            arv_time = np.array(self._arrival_time_period[key])
            
            arv_mean += arv_time.mean()
            arv_var += arv_time.var()
            
            self._arrival_time_period[key] = []
            self._rcv_msg_cnt[key] = 0
        arv_mean /= len(keys)
        arv_var /= len(keys)
        
        exec_time = np.array(self._execute_latency_period) / 1000
        self._execute_latency_period = []
        self._processing_latency_period = []
        for key in self._queue:
            self._arrival_time_period[key] = []
        self._throughput_period = 0
        

        return {
            'interarrival_time':{
                'mean': arv_mean,
                'var': arv_var
            },
            'service_time':{
                'mean': exec_time.mean(),
                'var': exec_time.var()
            }
        }
    
    def receive(self, source: str, msg: Message):
        """_summary_

        Args:
            source (str): vertex id of source task, which send this message
            msg (Message): _description_
        """
        if source in self._queue:
            if self._arrival_time[source]:
                self._arrival_time_period[source].append(msg.rcv_time - self._arrival_time[source][-1])
            self._arrival_time[source].append(msg.rcv_time)
            
            self._queue[source].append(msg)
            self._rcv_msg_cnt[source] += 1
            
    def _pop_data(self)-> Dict[str, Message]:
        ret: Dict[str, Message] = {}
        
        for key in self._queue:
            msg = self._queue[key].popleft()
            ret[key] = msg

        for key in ret:
            self._waiting_time[key].append((SystemClock.CURRENT - ret[key].rcv_time) * 1000)
       
        return ret
    
    def _processing(self):
        input:Dict[str, Message] = self._pop_data()
        latency = self._latency_generator.next() * (1 / self._speed_up)
        self._processing_latency.append(latency)
        self._processing_latency_period.append(latency)
        
        size_output= 0
        num_output = len(input) * self._selectivity
        max_delay = -1.0
        min_waiting_time = sys.maxsize
        for key in input:
            size_output += input[key].msg_size
            max_delay = max(max_delay, input[key].accumulated_latency)
            min_waiting_time = min(min_waiting_time, (SystemClock.CURRENT - input[key].rcv_time))

        size_output *= self._productivity
        msg = [Message(SystemClock.CURRENT, size_output, self._vertex_id, max_delay) for _ in range(num_output)]
        return msg, min_waiting_time
        
    def start(self):
        """_summary_
        """
        
        ret = None
        if self._ready():
            self._throughput_period += 1
            stime = time.time()
            res, waiting_time = self._processing() 
            execute_latency = self._processing_latency[-1] + (time.time() - stime)
            
            self._execute_latency.append(execute_latency)
            self._execute_latency_period.append(execute_latency)
            
            for msg in res:
                msg.update_accumulated_latency(self._execute_latency[-1])
            self._executable_time = SystemClock.CURRENT + self._execute_latency[-1] / 1000
            
            ret = {
                'msg': res,
                'execute_latency': self._execute_latency[-1],
                'processing_latency': self._processing_latency[-1]
            }
        
        return ret