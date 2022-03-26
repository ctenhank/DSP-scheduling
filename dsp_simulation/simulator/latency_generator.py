from abc import abstractmethod

class LatencyGenerator:
    """This class generates latency in the given latency_model, the default is gaussian noise model.
    """
    def __init__(self, jitter_model, mean=None, std=None) -> None:
        """Late

        Args:
            latency_model (_type_): _description_
            mean (_type_): _description_
            std (_type_): _description_
        """
        self._jitter_model = jitter_model
        self._mean = mean
        self._std = std
        
    @property
    def mean(self):
        return self._mean
    
    @mean.setter
    def mean(self, mean):
        self._mean = mean
        
    @property
    def std(self):
        return self._std
    
    @std.setter
    def std(self, std):
        self._std = std
        
    @abstractmethod
    def _get_noise(self):
        pass
    
    @abstractmethod
    def next_latency_ms(self):
        pass
    
class GaussianLatencyGenerator(LatencyGenerator):
    def __init__(self, jitter_model, mean=None, std=None) -> None:
        """Late

        Args:
            latency_model (_type_): _description_
            mean (_type_): _description_
            std (_type_): _description_
        """
        super().__init__(jitter_model, mean, std)
        self._jitter_model = jitter_model
        self._mean = mean
        self._std = std
        
    def _get_noise(self):
        return self._jitter_model()
    
    def next_latency_ms(self):
        return (self._get_noise() * self._std + self._mean) / 1000