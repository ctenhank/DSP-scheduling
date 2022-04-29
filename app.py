from copy import deepcopy
from select import select
from typing import List
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.topology.task_graph import TaskGraph

from dsp_simulation.topology.topology import Topology
from dsp_simulation.topology.vertex import OperatorVertex, SinkVertex, SourceVertex

from dsp_simulation.scheduler.rr_scheduler import RoundRobinScheduler
from dsp_simulation.scheduler.aco_scheduler import ACOScheduler
from dsp_simulation.scheduler.ga_scheduler import GAScheduler
from dsp_simulation.scheduler.rd_scheduler import RandomScheduler
from dsp_simulation.simulator.simulator import Simulator

import argparse
import datetime
from pathlib import Path


def wordcount_main():
    global args    
    
    topology = Topology(name=args.tp_name, conf_distribution='./conf/wc_latency_model.csv')
    
    source = SourceVertex(
        data_rate=10000,
        name='source'
        )
    split = OperatorVertex(
        parallelism=args.wc_st_hint, 
        selectivity=1, 
        productivity=1.1, 
        name="split"
        )
    count = OperatorVertex(
        parallelism=args.wc_cnt_hint, 
        selectivity=1, 
        productivity=1.1, 
        name="count"
        )
    #count2 = OperatorVertex(args.wc_cnt_hint, selectivity=1, productivity=1.1, name="count")
    #split = OperatorVertex(args.wc_st_hint, name="split")
    sink = SinkVertex(name='sink')

    topology.add_source(source)
    topology.add_operator(split)
    topology.add_operator(count)
    topology.add_sink(sink)

    topology.connect(source, split, 'shuffle')
    topology.connect(split, count, 'shuffle')
    topology.connect(count, sink, 'shuffle')    

    return topology

def join_model():
    pass

def linear_model():
    global args    
    
    topology = Topology(name=args.tp_name, conf_distribution='./conf/latency_model.csv')
    
    source = SourceVertex(datarate=5, name='source1', type_='source')    
    
    split = OperatorVertex(args.wc_st_hint, name="split", type='split')
    count = OperatorVertex(args.wc_cnt_hint, selectivity=1, productivity=1.1, name="count", type='count')
    
    sink = SinkVertex(name='sink')

    topology.add_source(source)
    topology.add_operator(split)
    topology.add_operator(count)
    topology.add_sink(sink)

    topology.connect(source, split, 'shuffle')
    topology.connect(split, count, 'shuffle')
    topology.connect(count, sink, 'shuffle')    

    return topology

def diamond_model():
    pass

def init_cluster():
    global args
    
    cluster: Cluster = Cluster(
       random=True, 
       max_node=args.cluster_max_node, 
       max_rack=args.cluster_max_rack, 
       max_worker=args.cluster_max_worker, 
       num_physical_nodes_per_rack=args.cluster_servers_per_rack
       )

    return cluster

def simulate(cluster: Cluster, topology: Topology):
    global args

    kr_tz = datetime.timezone(datetime.timedelta(hours=9))
    time = datetime.datetime.now(kr_tz)
    outdir = Path(f'./data/{time}')
    outdir.mkdir(exist_ok=True, parents=True)
    outdir = str(outdir)

    # task_graph = []
    # for t in topology:
    #     tg = TaskGraph(t.id, t.source, t.operator, t.sink, t.edge, args.max_operators_in_a_worker)
    #     # tg.make_taskgraph()
    #     task_graph.append(tg)
        # t.make_taskgraph()

    topology.instantiate(args.max_operators_in_a_worker)
    
    
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=20), outdir)
    ga_simulator.start_benchmark()
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=100), outdir)
    aco_simulator.start_benchmark()
    rr_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RoundRobinScheduler(), outdir, tot_time=args.simulation_time)
    rr_simulator.start_benchmark()
    rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), outdir)
    rd_simulator.start_benchmark()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--simulate-wordcount', action='store_true')
    parser.add_argument('--simulate-star', action='store_true')
    parser.add_argument('--simulate-dfas', action='store_true')
    
    parser.add_argument('--max-operators-in-a-worker', type=int, default=3)
    parser.add_argument('--cluster-max-node', type=int, default=50)
    parser.add_argument('--cluster-max-rack', type=int, default=8)
    parser.add_argument('--cluster-max-worker', type=int, default=10)
    parser.add_argument('--cluster-servers-per-rack', type=int, default=8)
    parser.add_argument('--tp-name', type=str, default='topology')
    parser.add_argument('--wc-src-res', type=int, default=24)
    parser.add_argument('--wc-st-res', type=int, default=16)
    parser.add_argument('--wc-cnt-res', type=int, default=10)
    parser.add_argument('--wc-src-hint', type=int, default=5)
    parser.add_argument('--wc-st-hint', type=int, default=8)
    parser.add_argument('--wc-cnt-hint', type=int, default=16)
    parser.add_argument('--data-rate', type=int, default=10000)
    parser.add_argument('--simulation-time', type=int, default=900)
    parser.add_argument('--auto-test', action='store_true')
    parser.add_argument('--mh-max-iteration', type=int, default=100)
    parser.add_argument('--mh-max-population', type=int, default=300)
    parser.add_argument('--mh-max-crossover', type=int, default=100)
    parser.add_argument('--mh-max-mutation', type=int, default=50)
    args = parser.parse_args()



    cluster = init_cluster()
    # job_graphs = []
    topology = None
    if args.simulate_wordcount:
        topology = wordcount_main()
        # job_graphs.append(wordcount_main())
    simulate(cluster, topology)