from metrics.latency_generator import LatencyGenerator, SomeAwesomeSimulator
import numpy
if __name__ == '__main__':
    
    mean, std = 0.7190926125335194, 0.1

    generator = LatencyGenerator(numpy.random.normal, mean, std)
    SomeAwesomeSimulator(generator).start_benchmark()
    pass