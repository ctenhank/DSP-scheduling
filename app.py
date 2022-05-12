from copy import deepcopy
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.runtime.profiler import Profiler
from dsp_simulation.simulator.generator import GaussianGenerator

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
import random as rd


def simple_linear_model():
    topology = Topology(name=args.tp_name, conf_distribution='./conf/latency_model.csv')
    source = SourceVertex(max_data_rate=10000, name='source')
    operator = OperatorVertex(parallelism=1, selectivity=1, productivity=1.1, name='op', latency_generator=GaussianGenerator(mean=2, std=0.2))
    operator2 = OperatorVertex(parallelism=1, selectivity=1, productivity=0.3, name='op2', latency_generator=GaussianGenerator(mean=0.78, std=0.12))
    operator3 = OperatorVertex(parallelism=1, selectivity=1, productivity=1.5, name='op3', latency_generator=GaussianGenerator(mean=1.2, std=0.15))
    sink = SinkVertex(name='sink')
    
    topology.add_source(source)
    topology.add_operator(operator)
    topology.add_operator(operator2)
    topology.add_operator(operator3)
    topology.add_sink(sink)
    
    topology.connect(source, operator, 'shuffle')
    topology.connect(operator, operator2, 'shuffle')
    topology.connect(operator2, operator3, 'shuffle')
    topology.connect(operator3, sink, 'shuffle')
    
    return topology
    
    
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
    
    topology.instantiate(args.max_operators_in_a_worker)
    profiler = Profiler(cluster=cluster, topology=topology)
    
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=1000), deepcopy(profiler), outdir, tot_time=args.simulation_time)
    aco_simulator.start_benchmark()
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=100), deepcopy(profiler), outdir, tot_time=args.simulation_time)
    ga_simulator.start_benchmark()
    rr_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RoundRobinScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time)
    rr_simulator.start_benchmark()
    rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time)
    rd_simulator.start_benchmark()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--simulate-wordcount', action='store_true')
    parser.add_argument('--simulate-star', action='store_true')
    parser.add_argument('--simulate-join', action='store_true')
    parser.add_argument('--simulate-linear', action='store_true')
    parser.add_argument('--simulate-replicated', action='store_true')
    parser.add_argument('--simulate-dynamic-input', action='store_true')
    
    parser.add_argument('--cluster-max-node', type=int, default=100)
    parser.add_argument('--cluster-max-rack', type=int, default=16)
    parser.add_argument('--cluster-max-worker', type=int, default=10)
    parser.add_argument('--cluster-servers-per-rack', type=int, default=8)
    parser.add_argument('--topology-name', type=str, default='topology')
    
    parser.add_argument('--max-operators-in-a-worker', type=int, default=3)
    
    parser.add_argument('--auto-test', action='store_true')
    parser.add_argument('--mh-max-iteration', type=int, default=100)
    parser.add_argument('--mh-max-population', type=int, default=300)
    parser.add_argument('--mh-max-crossover', type=int, default=100)
    parser.add_argument('--mh-max-mutation', type=int, default=50)    
    
    parser.add_argument('--simulation-time', type=int, default=900)
    parser.add_argument('--simulation-frequency', type=int, default=10000, help='Time frequency, if the value is 10000, simulator environment execute flow every 1 / frequency second')
    
    args = parser.parse_args()
    
    cluster = init_cluster()
    topology = []
    if args.simulate_linear:
        topology = simple_linear_model()
        
    simulate(cluster, topology)