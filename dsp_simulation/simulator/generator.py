from abc import abstractmethod
import numpy as np

class Generator:
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
    def next(self):
        pass
    
class GaussianGenerator(Generator):
    def __init__(self, mean=0.7190926125335194, std=0.1) -> None:
        """Late

        Args:
            mean (_type_): _description_
            std (_type_): _description_
        """
        super().__init__(None, mean, std)
        self._jitter_model = np.random.normal
        self._mean = mean
        self._std = std
        
    def _get_noise(self):
        return self._jitter_model()
    
    def next(self):
        return abs(self._get_noise() * self._std + self._mean)