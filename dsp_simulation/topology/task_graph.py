import random as rd
from typing import Dict, List, Tuple
from dsp_simulation.topology.grouping import GlobalGrouping, ShuffleGrouping
from dsp_simulation.topology.task import OperatorTask, SinkTask, SourceTask, Task
from dsp_simulation.topology.vertex import OperatorVertex, SinkVertex, SourceVertex, Vertex

class SubTaskGraph:
    CNT = 0
    def __init__(self, task: List[Task], edge: Dict[Task, Dict]):
        self._id = 'subgraph-' + str(SubTaskGraph.CNT)
        self._task = task
        self._edge = edge
        
        SubTaskGraph.CNT += 1
        
    def __str__(self):
        ret = ''
        ret += str(self._task) + '\n'
        ret += str(self._edge)
        return ret
    
    @property
    def id(self):
        return self._id

    @property
    def task(self):
        return self._task
    
    @property
    def edge(self):
        return self._edge

class TaskGraph:

    def __init__(self, 
    topology_id: str,
    source: List[SourceVertex], 
    operator: List[OperatorVertex], 
    sink: List[SinkVertex], 
    edge: Dict[Tuple[str, str], str],
    max_task:int=3
    ):
        """_summary_

        Args:
            source (List[SourceVertex]): _description_
            operator (List[OperatorVertex]): _description_
            sink (List[SinkVertex]): _description_
            edge (List[): _description_
            max_task (int, optional): The maximum number of tasks in a worker of a PhysicalNode. Defaults to 3.
        """
        self._topology_id = topology_id
        self._source = source
        self._operator = operator
        self._sink = sink
        self._edge = edge
        self._max_task = max_task
                
        self._task = {}
        self._task_edge = {}
        
        self._init_task()
        self._subgraph: List[SubTaskGraph] = self._partitioning()

    
    def __str__(self):
        ret = 'Task Graph\n'
        for sub in self._subgraph:
            ret += str(sub) + '\n'
        return ret    
    
    @property
    def subgraph(self):
        return self._subgraph
            

    def _get_vertex(self, vertex_id: str):
        for v in self._source:
            # print(v)
            if v.id == vertex_id:
                return v
            
        for v in self._operator:
            if v.id == vertex_id:
                return v

        for v in self._sink:
            if v.id == vertex_id:
                return v
        
        return None
    
    def _get_indegree(self, vertex: Vertex) -> List[str]:
        ret: List[str] = []
        for edge in self._edge:
            source = self._get_vertex(edge[0])
            target = self._get_vertex(edge[1])
            
            if target.id == vertex.id:
                ret.append(source.id)
        return ret

    def _init_task(self):
        for v in self._source:
            self._task[v.id] = [SourceTask(
                vertex_id=v.id,
                max_data_rate=v.data_rate,
                name=v.id + '-0',
                input_rate_dist=v.input_dist
                )]
            self._task_edge[self._task[v.id][0]] = {'target': [],
                        'rate': []}

        for v in self._sink:
            indegree = [v.id for v in v.indegree]
            self._task[v.id] = [SinkTask(v.id, indegree, v.id + '-0')]
            self._task_edge[self._task[v.id][0]] = {'target': [],
                        'rate': []}
        
        for v in self._operator:
            indegree = [v.id for v in v.indegree]
            self._task[v.id] = [OperatorTask(
                vertex_id=v.id,
                selectivity=v.selectivity,
                productivity=v.productivity, 
                indegree=indegree, 
                name=v.id+f'-{i}',
                latency_generator=v.latency_generator
                ) for i in range(v.parallelism)]
            for task in self._task[v.id]:
                self._task_edge[task] = {
                    'target': [],
                        'rate': []
                }
        
        for edge in self._edge:
            source = self._get_vertex(edge[0])
            target = self._get_vertex(edge[1])
            grp_type = self._edge[edge]
            
            if grp_type == 'shuffle':
                task_edge = ShuffleGrouping(self._task[source.id], self._task[target.id]).connect()
            elif grp_type == 'global':
                task_edge = GlobalGrouping(self._task[source.id], self._task[target.id]).connect()
            
            for tuple in task_edge:
                source = tuple[0][0]
                
                if source not in self._task_edge:
                    self._task_edge[source] = {
                        'target': [],
                        'rate': []
                    }
                
                
                destination = tuple[0][1]
                tranfer_rate = tuple[1]
                
                self._task_edge[source]['target'].append(destination)
                self._task_edge[source]['rate'].append(tranfer_rate)


    def _pin(self, vertex: List[Vertex]) -> List[List[Task]]:
        ret = []
        subgraph = []
        for v in vertex:
            subgraph.append(self._task[v.id][0])
        
        if len(subgraph) > 3:
            while len(subgraph) < 3:
                sub = subgraph[:3]
                ret.append(sub)
                subgraph = sub[3:]
        ret.append(subgraph)
        return ret


    def _mk_sub_task_graph(self, subgraph: List[Task]):
        subtask = []
        subtask_edge = {}
        for task in subgraph:
            subtask.append(task)
            subtask_edge[task] = self._task_edge[task]
            
        return SubTaskGraph(subtask, subtask_edge)


    def _partitioning(self):
        subgraphs: List[List[Task]] = []
        tasks = []
        
        source_subgraph: List[List[Task]] = self._pin(self._source)
        sink_subgraph: List[List[Task]] = self._pin(self._sink)
        
        subgraphs.extend(source_subgraph)
        subgraphs.extend(sink_subgraph)

        for vertex in self._operator:
            tasks.extend(self._task[vertex.id])

        selection = [i for i in range(len(tasks))]
        subgraph = []
        while len(selection) > 0:
            idx = rd.choice(selection)
            subgraph.append(tasks[idx])
            selection.remove(idx)

            if len(subgraph) >= self._max_task:
                subgraphs.append(subgraph)
                subgraph = []

        subgraphs.append(subgraph)  
        return [self._mk_sub_task_graph(subgraph) for subgraph in subgraphs]