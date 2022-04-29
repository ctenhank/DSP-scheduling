from dsp_simulation.simulator.latency_generator import GaussianLatencyGenerator
from dsp_simulation.topology.task import Task

class Network:
    """The mean and 
    """
    THREAD_MEAN = 0.004380266651921919
    PROCESS_MEAN = 0.019515183
    #NODE_MEAN = 35.955277
    NODE_MEAN = 0.17600326
    RACK_MEAN = NODE_MEAN * 2
    DCN_MEAN = 5.7412367
    THREAD_STD = 0.0006388322379872011
    PROCESS_STD = 0.004303672
    NODE_STD = 0.05225135
    #NODE_STD = 4.31235
    RACK_STD = NODE_STD * 2
    DCN_STD = 0.10867026
    
    TYPE = ['INTER-THREADS', 'INTER-PROCESS', 'INTER-NODE', 'INTER-RACK']
    SCALE = {
        'INTER-THREADS' : 1,
        'INTER-PROCESS' : 4,
        'INTER-NODE' : 8,
        'INTER-RACK' : 16
    }
    DISTRIBUTION = {
        'INTER-THREADS' : GaussianLatencyGenerator(THREAD_MEAN, THREAD_STD),
        'INTER-PROCESS' : GaussianLatencyGenerator(PROCESS_MEAN, PROCESS_STD),
        'INTER-NODE' : GaussianLatencyGenerator(NODE_MEAN, NODE_STD),
        'INTER-RACK' : GaussianLatencyGenerator(RACK_MEAN, RACK_STD)
    }
    
   

    #THREAD_DISTRIBUTION = GaussianLatencyGenerator(THREAD_MEAN, THREAD_STD)
    #PROCESS_DISTRIBUTION = GaussianLatencyGenerator(PROCESS_MEAN, PROCESS_STD)
    #NODE_DISTRIBUTION = GaussianLatencyGenerator(NODE_MEAN, NODE_STD)
    #RACK_DISTRIBUTION = GaussianLatencyGenerator(RACK_MEAN, RACK_STD)
    #DCN_DISTRIBUTION = GaussianLatencyGenerator(DCN_MEAN, DCN_STD)
    
    #def __init__(self, cluster):
    #    self._clsuter = cluster
    #    self._distribution = GaussianLatencyGenerator(Network.THREAD_MEAN, Network.THREAD_STD)
        
    #@property
    #def distribution(self):
    #    return self._distribution
        
    def check_communication_type(self, source: Task, target: Task):
        pass
        