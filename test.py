#from components.physical_node import PhysicalMachine
#from


#def test_pm_creation():
#    pm = PhysicalMachine()
#    print(pm.__id)
#    print(pm.id())

from components.cluster import Cluster
from components.topology import Topology

def test_random_cluster():
    cluster = Cluster(random=True)
    print(cluster)
    
def test_random_topology():
    topology = Topology(random=True)
    print(topology)
    
if __name__ == "__main__":
    #test_pm_creation()
    test_random_cluster()
    test_random_topology()