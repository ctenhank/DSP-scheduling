from copy import deepcopy
from ntpath import join
from dsp_simulation.cluster.cluster import Cluster
from dsp_simulation.runtime.profiler import Profiler
from dsp_simulation.scheduler.gwo_scheduler import GWOScheduler
from dsp_simulation.simulator.generator import GaussianGenerator

from dsp_simulation.topology.topology import Topology
from dsp_simulation.topology.vertex import OperatorVertex, SinkVertex, SourceVertex

from dsp_simulation.scheduler.rr_scheduler import RoundRobinScheduler
from dsp_simulation.scheduler.aco_scheduler import ACOScheduler
from dsp_simulation.scheduler.ga_scheduler import GAScheduler
from dsp_simulation.scheduler.rd_scheduler import RandomScheduler
from dsp_simulation.simulator.simulator import Simulator

from test import *

import argparse
import datetime
from pathlib import Path
import pickle as pkl


def word_count_topology():
    global args
    
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist='constant')
    source = SourceVertex(max_data_rate=5000, name='source')
    
    operator = OperatorVertex(parallelism=45, selectivity=10, productivity=0.1, name='split', latency_generator=GaussianGenerator(mean=8, std=1))
    operator2 = OperatorVertex(parallelism=35, selectivity=1, productivity=1, name='count', latency_generator=GaussianGenerator(mean=0.6, std=0.002))
    #operator3 = OperatorVertex(parallelism=1, selectivity=1, productivity=1.1, name='report', latency_generator=GaussianGenerator(mean=0.2, std=0.9))
    sink = SinkVertex(name='sink')
    
    topology.add_source(source)
    topology.add_operator(operator)
    topology.add_operator(operator2)
    #topology.add_operator(operator3)
    topology.add_sink(sink)
    
    topology.connect(source, operator, 'shuffle')
    topology.connect(operator, operator2, 'shuffle')
    topology.connect(operator2, sink, 'shuffle')
    #topology.connect(operator3, sink, 'shuffle')
    return topology


def word_count_topology2():
    global args
    
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=5000, name='source')
    
    operator = OperatorVertex(parallelism=1, selectivity=10, productivity=0.1, name='split', latency_generator=GaussianGenerator(mean=8, std=1))
    operator2 = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='count', latency_generator=GaussianGenerator(mean=0.6, std=0.002))
    #operator3 = OperatorVertex(parallelism=1, selectivity=1, productivity=1.1, name='report', latency_generator=GaussianGenerator(mean=0.2, std=0.9))
    sink = SinkVertex(name='sink')
    
    topology.add_source(source)
    topology.add_operator(operator)
    topology.add_operator(operator2)
    #topology.add_operator(operator3)
    topology.add_sink(sink)
    
    topology.connect(source, operator, 'shuffle')
    topology.connect(operator, operator2, 'shuffle')
    topology.connect(operator2, sink, 'shuffle')
    #topology.connect(operator3, sink, 'shuffle')
    return topology


def word_count_topology2_reference():
    global args
    
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=5000, name='source')
    
    #first exp
    #operator = OperatorVertex(parallelism=38, selectivity=10, productivity=0.1, name='split', latency_generator=GaussianGenerator(mean=8, std=1))
    #operator2 = OperatorVertex(parallelism=27, selectivity=1, productivity=1, name='count', latency_generator=GaussianGenerator(mean=0.6, std=0.002))
    
    #second exp
    operator = OperatorVertex(parallelism=25, selectivity=10, productivity=0.1, name='split', latency_generator=GaussianGenerator(mean=8, std=1))
    operator2 = OperatorVertex(parallelism=18, selectivity=1, productivity=1, name='count', latency_generator=GaussianGenerator(mean=0.6, std=0.002))

    sink = SinkVertex(name='sink')
    
    topology.add_source(source)
    topology.add_operator(operator)
    topology.add_operator(operator2)
    #topology.add_operator(operator3)
    topology.add_sink(sink)
    
    topology.connect(source, operator, 'shuffle')
    topology.connect(operator, operator2, 'shuffle')
    topology.connect(operator2, sink, 'shuffle')
    #topology.connect(operator3, sink, 'shuffle')
    return topology


def simplified_etl_application():
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=1000, name='source')
    
    SenMLParser = OperatorVertex(parallelism=1, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    RangeFilter = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='RangeFilter', latency_generator=GaussianGenerator(mean=3.7, std=0.18))
    BloomFilter = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='BloomFilter', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    Interpolation = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='Interpolation', latency_generator=GaussianGenerator(mean=5, std=0.6))
    Join = OperatorVertex(parallelism=1, selectivity=0.2, productivity=1.3, name='Join', latency_generator=GaussianGenerator(mean=1, std=0.1))
    Annotate = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='Annotate', latency_generator=GaussianGenerator(mean=3.3, std=0.3))
    CsvToSenML = OperatorVertex(parallelism=1, selectivity=1, productivity=2, name='CsvToSenML', latency_generator=GaussianGenerator(mean=1.5, std=0.07))
    sink = SinkVertex(name='sink')
    
    
    
    topology.add_source(source)
    topology.add_operator(SenMLParser)
    topology.add_operator(RangeFilter)
    topology.add_operator(BloomFilter)
    topology.add_operator(Interpolation)
    topology.add_operator(Join)
    topology.add_operator(Annotate)
    topology.add_operator(CsvToSenML)
    topology.add_sink(sink)

    
    topology.connect(source, SenMLParser, 'shuffle')
    topology.connect(SenMLParser, RangeFilter, 'shuffle')
    topology.connect(RangeFilter, BloomFilter, 'shuffle')
    topology.connect(BloomFilter, Interpolation, 'shuffle')
    topology.connect(Interpolation, Join, 'shuffle')
    #topology.connect(Interpolation, Annotate, 'shuffle')
    topology.connect(Join, Annotate, 'shuffle')
    topology.connect(Annotate, CsvToSenML, 'shuffle')
    topology.connect(CsvToSenML, sink, 'shuffle')
    return topology

def simplified_etl_application_reference():
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=1000, name='source')
    
    # first
    #SenMLParser = OperatorVertex(parallelism=1, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    #RangeFilter = OperatorVertex(parallelism=80, selectivity=1, productivity=1, name='RangeFilter', latency_generator=GaussianGenerator(mean=3.7, std=0.18))
    #BloomFilter = OperatorVertex(parallelism=6, selectivity=1, productivity=1, name='BloomFilter', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    #Interpolation = OperatorVertex(parallelism=20, selectivity=1, productivity=1, name='Interpolation', latency_generator=GaussianGenerator(mean=5, std=0.6))
    #Join = OperatorVertex(parallelism=1, selectivity=0.2, productivity=1.3, name='Join', latency_generator=GaussianGenerator(mean=1, std=0.1))
    #Annotate = OperatorVertex(parallelism=3, selectivity=1, productivity=1, name='Annotate', latency_generator=GaussianGenerator(mean=3.3, std=0.3))
    #CsvToSenML = OperatorVertex(parallelism=2, selectivity=1, productivity=2, name='CsvToSenML', latency_generator=GaussianGenerator(mean=1.5, std=0.07))
    
    
    # second
    SenMLParser = OperatorVertex(parallelism=1, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    RangeFilter = OperatorVertex(parallelism=60, selectivity=1, productivity=1, name='RangeFilter', latency_generator=GaussianGenerator(mean=3.7, std=0.18))
    BloomFilter = OperatorVertex(parallelism=5, selectivity=1, productivity=1, name='BloomFilter', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    Interpolation = OperatorVertex(parallelism=15, selectivity=1, productivity=1, name='Interpolation', latency_generator=GaussianGenerator(mean=5, std=0.6))
    Join = OperatorVertex(parallelism=1, selectivity=0.2, productivity=1.3, name='Join', latency_generator=GaussianGenerator(mean=1, std=0.1))
    Annotate = OperatorVertex(parallelism=2, selectivity=1, productivity=1, name='Annotate', latency_generator=GaussianGenerator(mean=3.3, std=0.3))
    CsvToSenML = OperatorVertex(parallelism=2, selectivity=1, productivity=2, name='CsvToSenML', latency_generator=GaussianGenerator(mean=1.5, std=0.07))
    
    sink = SinkVertex(name='sink')
    
    
    
    topology.add_source(source)
    topology.add_operator(SenMLParser)
    topology.add_operator(RangeFilter)
    topology.add_operator(BloomFilter)
    topology.add_operator(Interpolation)
    topology.add_operator(Join)
    topology.add_operator(Annotate)
    topology.add_operator(CsvToSenML)
    topology.add_sink(sink)

    
    topology.connect(source, SenMLParser, 'shuffle')
    topology.connect(SenMLParser, RangeFilter, 'shuffle')
    topology.connect(RangeFilter, BloomFilter, 'shuffle')
    topology.connect(BloomFilter, Interpolation, 'shuffle')
    topology.connect(Interpolation, Join, 'shuffle')
    #topology.connect(Interpolation, Annotate, 'shuffle')
    topology.connect(Join, Annotate, 'shuffle')
    topology.connect(Annotate, CsvToSenML, 'shuffle')
    topology.connect(CsvToSenML, sink, 'shuffle')
    return topology

def simplified_stats_application():
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=5000, name='source')
    
    SenMLParser = OperatorVertex(parallelism=1, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    LinearRegression = OperatorVertex(parallelism=1, selectivity=1, productivity=2, name='SildingLinearRegression', latency_generator=GaussianGenerator(mean=2, std=0.18))
    BlackWindowAvg = OperatorVertex(parallelism=1, selectivity=0.5, productivity=1, name='Average', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    DistinctApproximationCount = OperatorVertex(parallelism=1, selectivity=1, productivity=1, name='DistinctCount', latency_generator=GaussianGenerator(mean=3, std=0.6))
    MultiLinePlot = OperatorVertex(parallelism=1, selectivity=0.2, productivity=0.0025, name='MultilinePlot', latency_generator=GaussianGenerator(mean=1, std=0.1))
    sink = SinkVertex(name='sink')

    topology.add_source(source)
    topology.add_operator(SenMLParser)
    topology.add_operator(LinearRegression)
    topology.add_operator(BlackWindowAvg)
    topology.add_operator(DistinctApproximationCount)
    topology.add_operator(MultiLinePlot)
    topology.add_sink(sink)
    
    topology.connect(source, SenMLParser, 'shuffle')
    topology.connect(SenMLParser, LinearRegression, 'shuffle')
    topology.connect(SenMLParser, BlackWindowAvg, 'shuffle')
    topology.connect(SenMLParser, DistinctApproximationCount, 'shuffle')
    topology.connect(LinearRegression, DistinctApproximationCount, 'shuffle')
    topology.connect(BlackWindowAvg, DistinctApproximationCount, 'shuffle')
    topology.connect(DistinctApproximationCount, MultiLinePlot, 'shuffle')
    topology.connect(MultiLinePlot, sink, 'shuffle')
    return topology

def simplified_stats_application_reference():
    topology = Topology(name=args.topology_name, step=args.simulation_time, input_rate_dist=args.input_dist)
    source = SourceVertex(max_data_rate=5000, name='source')
    
    # first exp
    #SenMLParser = OperatorVertex(parallelism=5, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    #LinearRegression = OperatorVertex(parallelism=142, selectivity=1, productivity=2, name='SildingLinearRegression', latency_generator=GaussianGenerator(mean=2, std=0.18))
    #BlackWindowAvg = OperatorVertex(parallelism=8, selectivity=0.5, productivity=1, name='Average', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    #DistinctApproximationCount = OperatorVertex(parallelism=55, selectivity=1, productivity=1, name='DistinctCount', latency_generator=GaussianGenerator(mean=3, std=0.6))
    #MultiLinePlot = OperatorVertex(parallelism=3, selectivity=0.2, productivity=0.0025, name='MultilinePlot', latency_generator=GaussianGenerator(mean=1, std=0.1))
    
    # second exp
    SenMLParser = OperatorVertex(parallelism=4, selectivity=5, productivity=0.33, name='SenMLParser', latency_generator=GaussianGenerator(mean=1, std=0.05))
    LinearRegression = OperatorVertex(parallelism=120, selectivity=1, productivity=2, name='SildingLinearRegression', latency_generator=GaussianGenerator(mean=2, std=0.18))
    BlackWindowAvg = OperatorVertex(parallelism=6, selectivity=0.5, productivity=1, name='Average', latency_generator=GaussianGenerator(mean=0.85, std=0.02))
    DistinctApproximationCount = OperatorVertex(parallelism=45, selectivity=1, productivity=1, name='DistinctCount', latency_generator=GaussianGenerator(mean=3, std=0.6))
    MultiLinePlot = OperatorVertex(parallelism=2, selectivity=0.2, productivity=0.0025, name='MultilinePlot', latency_generator=GaussianGenerator(mean=1, std=0.1))
    
    sink = SinkVertex(name='sink')

    topology.add_source(source)
    topology.add_operator(SenMLParser)
    topology.add_operator(LinearRegression)
    topology.add_operator(BlackWindowAvg)
    topology.add_operator(DistinctApproximationCount)
    topology.add_operator(MultiLinePlot)
    topology.add_sink(sink)
    
    topology.connect(source, SenMLParser, 'shuffle')
    topology.connect(SenMLParser, LinearRegression, 'shuffle')
    topology.connect(SenMLParser, BlackWindowAvg, 'shuffle')
    topology.connect(SenMLParser, DistinctApproximationCount, 'shuffle')
    topology.connect(LinearRegression, DistinctApproximationCount, 'shuffle')
    topology.connect(BlackWindowAvg, DistinctApproximationCount, 'shuffle')
    topology.connect(DistinctApproximationCount, MultiLinePlot, 'shuffle')
    topology.connect(MultiLinePlot, sink, 'shuffle')
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

import multiprocessing as mlp

def simulate(cluster: Cluster, topology: Topology):
    global args

    kr_tz = datetime.timezone(datetime.timedelta(hours=9))
    time = datetime.datetime.now(kr_tz)
    
    if args.output_directory:
        outdir = Path(f'{args.output_directory}/{time}')
    else:
        outdir = Path(f'./log/{time}')
    outdir.mkdir(exist_ok=True, parents=True)
    outdir = str(outdir)
    
    #topology.instantiate(args.max_operators_in_a_worker)
    profiler = Profiler(cluster=cluster, topology=topology)
    

    rr_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RoundRobinScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    rr_simulator.start_benchmark()
    #rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    #rd_simulator.start_benchmark()
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=200, alpha=3, beta=1), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    aco_simulator.start_benchmark()
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=25, num_pop=100, num_cross=33, num_mut=33), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    ga_simulator.start_benchmark()
    gwo_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GWOScheduler(num_wolves=75, num_iter=50), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    gwo_simulator.start_benchmark()
    

def simulate2(cluster: Cluster, topology: Topology):
    global args

    kr_tz = datetime.timezone(datetime.timedelta(hours=9))
    time = datetime.datetime.now(kr_tz)
    
    if args.output_directory:
        outdir = Path(f'{args.output_directory}/{time}')
    else:
        outdir = Path(f'./log/{time}')
    outdir.mkdir(exist_ok=True, parents=True)
    outdir = str(outdir)
    
    #topology.instantiate(args.max_operators_in_a_worker)
    profiler = Profiler(cluster=cluster, topology=topology)
    
    rr_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RoundRobinScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    rr_simulator.start_benchmark()
    #rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    #rd_simulator.start_benchmark()
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=100, alpha=3, beta=1), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    aco_simulator.start_benchmark()
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=25, num_pop=50, num_cross=15, num_mut=15), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    ga_simulator.start_benchmark()
    gwo_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GWOScheduler(num_wolves=75, num_iter=25), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    gwo_simulator.start_benchmark()
    
def simulate4(cluster: Cluster, topology: Topology, ref_topology:Topology):
    global args

    kr_tz = datetime.timezone(datetime.timedelta(hours=9))
    time = datetime.datetime.now(kr_tz)
    
    if args.output_directory:
        outdir = Path(f'{args.output_directory}/{time}')
    else:
        outdir = Path(f'./log/{time}')
    outdir.mkdir(exist_ok=True, parents=True)
    outdir = str(outdir)
    
    #topology.instantiate(args.max_operators_in_a_worker)
    profiler = Profiler(cluster=cluster, topology=topology)
    
    rr_simulator = Simulator(deepcopy(cluster), deepcopy(ref_topology), RoundRobinScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time)
    rr_simulator.start_benchmark()
    #rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    #rd_simulator.start_benchmark()
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=100, alpha=3, beta=1), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    aco_simulator.start_benchmark()
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=25, num_pop=50, num_cross=15, num_mut=15), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    ga_simulator.start_benchmark()
    gwo_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GWOScheduler(num_wolves=75, num_iter=25), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    gwo_simulator.start_benchmark()
    

def simulate3(cluster: Cluster, topology: Topology):
    global args

    kr_tz = datetime.timezone(datetime.timedelta(hours=9))
    time = datetime.datetime.now(kr_tz)
    
    if args.output_directory:
        outdir = Path(f'{args.output_directory}/{time}')
    else:
        outdir = Path(f'./log/{time}')
    outdir.mkdir(exist_ok=True, parents=True)
    outdir = str(outdir)
    
    #topology.instantiate(args.max_operators_in_a_worker)
    profiler = Profiler(cluster=cluster, topology=topology)
    
    
    rr_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RoundRobinScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    rr_simulator.start_benchmark()
    #rd_simulator = Simulator(deepcopy(cluster), deepcopy(topology), RandomScheduler(), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    #rd_simulator.start_benchmark()
    aco_simulator = Simulator(deepcopy(cluster), deepcopy(topology), ACOScheduler(num_ants=100, alpha=3, beta=1), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    aco_simulator.start_benchmark()
    ga_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GAScheduler(num_iter=25, num_pop=50, num_cross=15, num_mut=15), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    ga_simulator.start_benchmark()
    gwo_simulator = Simulator(deepcopy(cluster), deepcopy(topology), GWOScheduler(num_wolves=75, num_iter=25), deepcopy(profiler), outdir, tot_time=args.simulation_time, runtime=args.runtime)
    gwo_simulator.start_benchmark()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--simulate-wordcount', action='store_true')
    parser.add_argument('--simulate-wordcount2', action='store_true')
    parser.add_argument('--simulate-star', action='store_true')
    #parser.add_argument('--simulate-join', action='store_true')
    parser.add_argument('--simulate-linear', action='store_true')
    parser.add_argument('--simulate-linear2', action='store_true')
    parser.add_argument('--simulate-linear3', action='store_true')
    parser.add_argument('--simulate-linear4', action='store_true')
    parser.add_argument('--simulate-replicated', action='store_true')
    parser.add_argument('--simulate-join', action='store_true')
    parser.add_argument('--simulate-etl', action='store_true')
    parser.add_argument('--simulate-stat', action='store_true')
    parser.add_argument('--simulate-dynamic-input', action='store_true')
    parser.add_argument('--benchmark', action='store_true')
    parser.add_argument('--benchmark-count', default=0, type=int)
    
    parser.add_argument('--cluster-max-node', type=int, default=100)
    parser.add_argument('--cluster-max-rack', type=int, default=16)
    parser.add_argument('--cluster-max-worker', type=int, default=10)
    parser.add_argument('--cluster-servers-per-rack', type=int, default=8)
    parser.add_argument('--topology-name', type=str, default='topology')
    
    parser.add_argument('--save', action='store_true')
    
    parser.add_argument('--max-operators-in-a-worker', type=int, default=3)
    
    parser.add_argument('--auto-test', action='store_true')
    parser.add_argument('--mh-max-iteration', type=int, default=100)
    parser.add_argument('--mh-max-population', type=int, default=300)
    parser.add_argument('--mh-max-crossover', type=int, default=100)
    parser.add_argument('--mh-max-mutation', type=int, default=50)    
    
    parser.add_argument('--num-wolves', type=int, default=5)    
    
    parser.add_argument('--simulation-time', type=int, default=900)
    parser.add_argument('--simulation-frequency', type=int, default=10000, help='Time frequency, if the value is 10000, simulator environment execute flow every 1 / frequency second')
    
    parser.add_argument('--output-directory', type=str)
    
    parser.add_argument('--test-aco-num-ant', action='store_true')
    parser.add_argument('--test-aco-ctl', action='store_true')
    parser.add_argument('--test-aco-num-iter', action='store_true')
    parser.add_argument('--test-ga-num-iter', action='store_true')
    parser.add_argument('--test-ga-num-pop', action='store_true')
    parser.add_argument('--test-gwo-num-wolf', action='store_true')
    parser.add_argument('--test-gwo-num-iter', action='store_true')
    parser.add_argument('--test-overall', action='store_true')
    parser.add_argument('--test-first', action='store_true')
    parser.add_argument('--test-second', action='store_true')
    parser.add_argument('--test-count', type=int, default=10)
    #test_overall
    #parser.add_argument('--test-gwo-num-iter', action='store_true')
    
    parser.add_argument('--runtime', default=False, action='store_true')
    parser.add_argument('--load', action='store_true')
    parser.add_argument('--random-cluster', action='store_true')
    
    parser.add_argument('--benchmark-static', action='store_true')
    parser.add_argument('--benchmark-reference', action='store_true')
    parser.add_argument('--input-dist', type=str, default='binomial')
    
    args = parser.parse_args()
    
    if args.random_cluster:
        cluster=  init_cluster()
    cluster = init_cluster()
    topology = []
    if args.simulate_wordcount:
        topology = word_count_topology()
    if args.simulate_wordcount2:
        topology = word_count_topology2()
    if args.simulate_linear:
        topology = simple_linear_model()
    if args.simulate_linear2:
        topology = simple_linear_model2()
    if args.simulate_linear3:
        topology = simple_linear_model3()
    if args.simulate_linear4:
        topology = simple_linear_model4()
    if args.simulate_join:
        topology = join_test()
    if args.simulate_etl:
        topology = simplified_etl_application()
    if args.simulate_stat:
        topology = simplified_stats_application()
    
    if args.save:
        
        topology.instantiate(args.max_operators_in_a_worker)
        with open('test-cluster.pkl', 'wb') as f:
            pkl.dump(cluster, f)
        with open('test-topology.pkl', 'wb') as f:
            pkl.dump(topology, f)
            
    if args.load:
        with open('test-cluster.pkl', 'rb') as f:
            cluster: Cluster = pkl.load(f)
            cluster.initialize_objective()
        with open('test-topology.pkl', 'rb') as f:
            topology = pkl.load(f)
    
    if args.test_aco_num_ant:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_aco_algorithm_by_num_ants(cluster, topology, args)    
    elif args.test_aco_ctl:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_aco_algorithm_by_alpha_beta(cluster, topology, args)
    elif args.test_aco_num_iter:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_aco_algorithm_by_num_iter(cluster, topology, args)
    elif args.test_ga_num_iter:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_ga_num_iter(cluster, topology, args) 
    elif args.test_ga_num_pop:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_ga(cluster, topology, args) 
    elif args.test_gwo_num_wolf:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_gwo_by_num_wolf(cluster, topology, args)
    elif args.test_gwo_num_iter:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.test_count):
            simulate_gwo_by_iteration(cluster, topology, args)
    
    #simulate_ga(cluster ,topology, args)
    
    #test_gwo(cluster, topology, args)
    #if args.simulate_etl:
    #    topology = simplified_etl_application()
    
    #for _ in range(args.benchmark_count):
    #    topology.instantiate(args.max_operators_in_a_worker)
    #    if args.benchmark:
    #        simulate(cluster, topology)
            
    #if args.simulate_stat:
    #    topology = simplified_stats_application()
    #    for _ in range(args.benchmark_count):
    #        topology.instantiate(args.max_operators_in_a_worker)
    #    if args.benchmark:
    #        simulate(cluster, topology)
            
    #if args.simulate_etl:
    #    topology = simplified_etl_application()
    
    #for _ in range(args.benchmark_count):
    #    topology.instantiate(args.max_operators_in_a_worker)
    #    if args.benchmark:
    #        simulate(cluster, topology)
    
    if args.test_overall:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(1000):
            simulate_aco_algorithm_by_num_ants(cluster, topology, args)  
            simulate_aco_algorithm_by_alpha_beta(cluster, topology, args)
            simulate_ga(cluster, topology, args) 
            simulate_ga_num_iter(cluster, topology, args) 
            simulate_gwo_by_num_wolf(cluster,topology,args)
            simulate_gwo_by_iteration(cluster,topology,args)
    
    if args.test_first:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(1000):
            simulate_aco_algorithm_by_num_ants(cluster, topology, args)  
            simulate_ga(cluster, topology, args) 
            simulate_gwo_by_num_wolf(cluster,topology,args)
    
    if args.test_second:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(1000):
            simulate_aco_algorithm_by_num_ants(cluster, topology, args)  
            simulate_aco_algorithm_by_alpha_beta(cluster, topology, args)
            simulate_ga(cluster, topology, args) 
            simulate_ga_num_iter(cluster, topology, args) 
            simulate_gwo_by_num_wolf(cluster,topology,args)
            simulate_gwo_by_iteration(cluster,topology,args)
            
    if args.benchmark_static:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.benchmark_count):
            simulate3(cluster, topology)
    elif args.benchmark_reference:
        topology.instantiate(args.max_operators_in_a_worker)
        if args.simulate_wordcount2:
            ref_topology = word_count_topology2_reference()
        if args.simulate_etl:
            ref_topology = simplified_etl_application_reference()
        if args.simulate_stat:
            ref_topology = simplified_stats_application_reference()
        ref_topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.benchmark_count):
            simulate4(cluster, topology, ref_topology)
        
    elif args.benchmark:
        topology.instantiate(args.max_operators_in_a_worker)
        for _ in range(args.benchmark_count):
            simulate2(cluster, topology)
        
        
        
    #if args.simulate_stat:
    #    for _ in range(args.benchmark_count):
    #        topology = simplified_stats_application()
    #        topology.instantiate(args.max_operators_in_a_worker)
    #        simulate2(cluster, topology)
    #elif args.simulate_etl:
    #    for _ in range(args.benchmark_count):
    #        topology = simplified_etl_application()
    #        topology.instantiate(args.max_operators_in_a_worker)
    #        simulate2(cluster, topology)
    #else:
    #    for _ in range(args.benchmark_count):
    #        topology.instantiate(args.max_operators_in_a_worker)
    #        simulate2(cluster, topology)
                
    #for _ in range(args.benchmark_count):
    #    if args.simulate_stat:
    #        topology = simplified_stats_application()
    #    topology.instantiate(args.max_operators_in_a_worker)
    #    if args.benchmark:
    #        simulate2(cluster, topology)
            
    #    if args.simulate_etl:
    #        topology = simplified_etl_application()
    #    topology.instantiate(args.max_operators_in_a_worker)
        #if args.benchmark:
        #    simulate2(cluster, topology)
                
                