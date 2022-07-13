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

#tz = datetime.timezone(datetime.timedelta(hours=9))
#now = datetime.datetime.now(tz)
#outdir = Path(f'./log/{now}')


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
    def shutdown(self, outdir):
        pass

    @property
    def id(self):
        return self._id

    @property
    def vertex_id(self):
        return self._vertex_id


class SourceTask(Task):
    def __init__(self, vertex_id, max_data_rate, out_degree, name=None, dsize_mean=250, dsize_std=25, input_rate_dist=None):
        super().__init__(vertex_id, name)

        self._max_data_rate = max_data_rate
        self._current_data_rate = int(
            input_rate_dist[int(SystemClock.CURRENT)] * self._max_data_rate)
        self._time_for_data = 1 / self._current_data_rate
        self._input_rate_dist = input_rate_dist

        self._data_size = []
        self._data_size_gernerator = GaussianGenerator(dsize_mean, dsize_std)
        
        self._sent_msg_cnt = []
        self._sent_msg_cnt_period = 0
        self._last_executed = 0
        self._out_degree = out_degree
        
    @property
    def sent_msg_cnt_period(self):
        return self._sent_msg_cnt_period
    
    @property
    def out_degree(self):
        return self._out_degree
        
    def receive(self, source: str, msg: Message):
        pass

    def _update_data_rate(self):
        self._current_data_rate = int(
            self._max_data_rate * self._input_rate_dist[int(SystemClock.CURRENT)])
        self._time_for_data = 1 / self._current_data_rate

    def post_result(self):
        #print(f'{self._id} ({int(SystemClock.CURRENT)}): Sent message count({self._sent_msg_cnt})')
        self._sent_msg_cnt.append(self._sent_msg_cnt_period)
        self._sent_msg_cnt_period = 0
        self._last_executed = 0
        self._round_start_time = SystemClock.CURRENT

        self._update_data_rate()

        return {
            'reporter':{
                'sent_msg_cnt': self._sent_msg_cnt[-1]
            }
        }

    def shutdown(self, outdir):
        basedir: Path = Path(outdir) / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir / (self._id + '.pkl')

        obj = {
            'max_data_rate': self._max_data_rate,
            'data_size': self._data_size,
            'data_rate_distribution': self._input_rate_dist,
            'sent_msg_cnt': self._sent_msg_cnt
        }

        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)

        #print(f'Writing a log file to {filepath}')

    def start(self):
        current = int((SystemClock.CURRENT % 1) / self._time_for_data) + 1

        if current > self._last_executed:
            msg_size = self._data_size_gernerator.next()
            self._data_size.append(msg_size)
            self._sent_msg_cnt_period += 1
            self._last_executed = current

            return {
                'msg': Message(
                    event_time=SystemClock.CURRENT,
                    msg_size=msg_size,
                    vertex_id=self.vertex_id
                )
            }
            
    def fake_start(self, fake):
        current = int((fake % 1) / self._time_for_data) + 1

        if current > self._last_executed:
            #print(f'SourceTask fake: {fake}')
            msg_size = self._data_size_gernerator.next()
            self._data_size.append(msg_size)
            self._sent_msg_cnt_period += 1
            self._last_executed = current

            return {
                'msg': Message(
                    event_time=fake,
                    msg_size=msg_size,
                    vertex_id=self.vertex_id
                )
            }


class SinkTask(Task):
    def __init__(self, vertex_id, name=None):
        super().__init__(vertex_id, name)

        self._throughput = []
        self._throughput_period = 0
        self._end_to_end_delay: List[float] = []
        self._end_to_end_delay_period: List[float] = []
        self._queue: Deque[Message] = deque()

    def post_result(self):
        self._throughput.append(self._throughput_period)
        self._end_to_end_delay.append(
            np.array(self._end_to_end_delay_period).mean())

        self._throughput_period = 0
        self._end_to_end_delay_period = []

        return {
            'reporter':{
                'throughput': self._throughput[-1],
                'end_to_end_delay': self._end_to_end_delay[-1]   
            }
        }

    def start(self):
        while self._queue:
            e = self._queue.pop()
            self._end_to_end_delay_period.append(e.accumulated_latency)
            self._throughput_period += 1
        return None

    def shutdown(self, outdir):
        basedir: Path = Path(outdir) / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir / (self._id + '.pkl')

        obj = {
            'throughput': self._throughput,
            'e2e_delay': self._end_to_end_delay
        }

        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)

    def receive(self, source: str, msg: Message):
        self._queue.append(msg)


class OperatorTask(Task):
    def __init__(self, vertex_id, selectivity, productivity, indegree: List[str] = None, out_degree: List[str]=None, name=None, latency_generator: Generator = None):
        """_summary_

        Args:
            vertex_id (_type_): _description_
            selectivity (_type_): _description_
            productivity (_type_): _description_
            indegree (List[str], optional): _description_. Defaults to None.
            name (_type_, optional): _description_. Defaults to None.
            cap (_type_, optional): _description_. Defaults to None.
            latency_generator (Generator, optional): _description_. Defaults to None.
        """
        super().__init__(vertex_id, name)

        self._speed_up = None
        self._selectivity = selectivity
        self._required_num_tuple = 1
        if selectivity < 1:
            self._required_num_tuple = int(1 / selectivity) 
            
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
        self._waiting_time: Dict[str, List] = {}
        self._arrival_time: Dict[str, List] = {}
        self._arrival_time_period: Dict[str, List] = {}
        self._out_degree = out_degree

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
        for thr in self._throughput:
            ret += thr
        return ret
    
    @property
    def out_degree(self):
        return self._out_degree
    
    def update_speed_up(self, speed_up):
        self._speed_up = speed_up

    def shutdown(self, outdir):
        basedir: Path = Path(outdir) / self.vertex_id
        basedir.mkdir(exist_ok=True, parents=True)
        filepath = basedir / (self._id + '.pkl')

        #for key in self._waiting_time:
        #    print(f'Average waiting time({key}->{self.vertex_id}): {np.array(self._waiting_time[key]).mean()}ms')

        obj = {
            'throughput': self._throughput,
            'execute_latency': self._execute_latency,
            'processing_latency': self._processing_latency,
            'productivity': self._productivity,
            'selectivity': self._selectivity,
            'speed_up': self._speed_up,
            'received_messasge': self._rcv_msg_cnt,
            'sent_message': self._snd_msg_cnt,
            #'waiting_time': self._waiting_time[key]
        }

        with filepath.open('wb') as f:
            pickle.dump(obj, file=f)
        #print(f'Writing a log file to {filepath}')

    def _ready(self):
        """If there are various incoming data from multiple preceding operators, it should be blocked up to there is a data for each queue

        Returns:
            _type_: _description_
        """
        if self._executable_time > SystemClock.CURRENT:
            return False

        for key in self._queue:
            if len(self._queue[key]) < self._required_num_tuple:
                return False
            if SystemClock.CURRENT < self._queue[key][0].rcv_time:
                return False

        return True

    def post_result(self):

        #print(f'{self._id} ({int(SystemClock.CURRENT)}): throughput({self._throughput})')

        arv_mean, arv_var = 0, 0
        keys = []
        
        for key in self._queue:
            keys.append(key)
            #if not self._arrival_time_period[key]:
            #    print(f'asdfasdfasdf {self.id}, {key}: {self._arrival_time_period[key]}')
            arv_time = np.array(self._arrival_time_period[key])
            #print(f'{self.id} arv_time: {key}, {arv_time.mean()}')

            arv_mean += arv_time.mean()
            arv_var += arv_time.var()

            self._arrival_time_period[key] = []
            self._rcv_msg_cnt[key] = 0
        arv_mean /= len(keys)
        arv_var /= len(keys)

        exec_time = np.array(self._execute_latency_period) / 1000
        

        self._throughput.append(self._throughput_period)
        self._execute_latency.append(np.mean(self._execute_latency_period))
        self._processing_latency.append(
            np.mean(self._processing_latency_period))
        
        
        #print(f'{self.id} throughput: {self._throughput_period}')
        #print(f'{self.id} exec_time: {exec_time.mean()}')
        #print(f'{self.id} arv_time: {np.array(self._arrival_time_period[key]).mean()}')
        

        self._execute_latency_period = []
        self._processing_latency_period = []
        self._throughput_period = 0

        for key in self._queue:
            self._arrival_time_period[key] = []

        return {
            'profiler': {
                'interarrival_time': {
                    'mean': arv_mean,
                    'var': arv_var
                },
                'service_time': {
                    'mean': exec_time.mean(),
                    'var': exec_time.var()
                }
            },
            'reporter': {
                'throughput': self._throughput[-1],
                'processing_latency': self._processing_latency[-1],
                'execute_latency': self._execute_latency[-1]
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
                self._arrival_time_period[source].append(
                    (msg.rcv_time - self._arrival_time[source][-1])*self._required_num_tuple)
            self._arrival_time[source].append(msg.rcv_time)

            self._queue[source].append(msg)
            self._rcv_msg_cnt[source] += 1
        #elif 

    def _pop_data(self) -> Dict[str, List[Message]]:
        ret: Dict[str, List[Message]] = {}

        for key in self._queue:
            ret[key] = []
            for _ in range(self._required_num_tuple):
                msg = self._queue[key].popleft()
                ret[key].append(msg)

        for key in ret:
            for i in range(self._required_num_tuple):
                self._waiting_time[key].append(
                    (SystemClock.CURRENT - ret[key][i].rcv_time) * 1000)

        return ret

    def _processing(self):
        input: Dict[str, List[Message]] = self._pop_data()
        latency = self._latency_generator.next() * (1 / self._speed_up)
        self._processing_latency_period.append(latency)

        size_output = 0
        #num_output = int(len(input) * self._selectivity)
        
        max_delay = -1.0
        min_waiting_time = sys.maxsize
        for key in input:
            #print(len(input[key]))
            num_output = int(len(input[key]) * self._selectivity)
            key_output = 0
            for i in range(self._required_num_tuple):
                key_output += input[key][i].msg_size
                max_delay = max(max_delay, input[key][i].accumulated_latency)
                min_waiting_time = min(
                    min_waiting_time, (SystemClock.CURRENT - input[key][i].rcv_time)/ 1000)
            size_output += key_output / self._required_num_tuple
            
        max_delay += min_waiting_time
        size_output *= self._productivity
        #print(num_output)
        msg = [Message(SystemClock.CURRENT, size_output,
                       self._vertex_id, max_delay) for _ in range(num_output)]
        return msg, min_waiting_time

    def start(self):
        """_summary_
        """

        ret = None
        if self._ready():
            self._throughput_period += 1
            stime = time.time()
            res, waiting_time = self._processing()
            execute_latency = self._processing_latency_period[-1] + (
                time.time() - stime)
            self._execute_latency_period.append(execute_latency)

            for msg in res:
                msg.update_accumulated_latency(
                    self._execute_latency_period[-1])

            self._executable_time = SystemClock.CURRENT + \
                self._execute_latency_period[-1] / 1000

            ret = {
                'msg': res,
                'execute_latency': self._execute_latency_period[-1],
                'processing_latency': self._processing_latency_period[-1]
            }

        return ret
    
    def _fake_ready(self, fake):
        """If there are various incoming data from multiple preceding operators, it should be blocked up to there is a data for each queue

        Returns:
            _type_: _description_
        """
        if self._executable_time > fake:
            return False

        for key in self._queue:
            if len(self._queue[key]) < self._required_num_tuple:
                return False
            if fake < self._queue[key][0].rcv_time:
                return False

        return True
    
    def _fake_pop_data(self, fake) -> Dict[str, List[Message]]:
        ret: Dict[str, List[Message]] = {}

        for key in self._queue:
            ret[key] = []
            for _ in range(self._required_num_tuple):
                msg = self._queue[key].popleft()
                ret[key].append(msg)

        for key in ret:
            for i in range(self._required_num_tuple):
                self._waiting_time[key].append(
                    (fake - ret[key][i].rcv_time) * 1000)

        return ret

    def _fake_processing(self, fake):
        input: Dict[str, List[Message]] = self._pop_data()
        latency = self._latency_generator.next() * (1 / self._speed_up)
        self._processing_latency_period.append(latency)

        size_output = 0
        #num_output = int(len(input) * self._selectivity)
        
        max_delay = -1.0
        min_waiting_time = sys.maxsize
        for key in input:
            #print(len(input[key]))
            num_output = int(len(input[key]) * self._selectivity)
            key_output = 0
            for i in range(self._required_num_tuple):
                key_output += input[key][i].msg_size
                max_delay = max(max_delay, input[key][i].accumulated_latency)
                min_waiting_time = min(
                    min_waiting_time, (fake - input[key][i].rcv_time) / 1000)
            size_output += key_output / self._required_num_tuple
            
        max_delay += min_waiting_time
        size_output *= self._productivity
        #print(num_output)
        msg = [Message(fake, size_output,
                       self._vertex_id, max_delay) for _ in range(num_output)]
        return msg, min_waiting_time
    
    def fake_start(self, fake):
        ret = None
        if self._fake_ready(fake):
            #print(f'OperatorTask fake: {self.id}, {fake}')
            #print(f)
            self._throughput_period += 1
            stime = time.time()
            res, waiting_time = self._fake_processing(fake)
            execute_latency = self._processing_latency_period[-1] + (
                time.time() - stime)
            self._execute_latency_period.append(execute_latency)

            for msg in res:
                msg.update_accumulated_latency(
                    self._execute_latency_period[-1])

            self._executable_time = fake + \
                self._execute_latency_period[-1] / 1000

            ret = {
                'msg': res,
                'execute_latency': self._execute_latency_period[-1],
                'processing_latency': self._processing_latency_period[-1]
            }

        return ret