import numpy as np

def get_value(mu, sigma):
    ret = np.random.normal(mu, sigma)
    while ret < .0 or ret > 1.0:
        ret = np.random.normal(mu, sigma)
    return ret

#def binomal_distribution(seconds=900):
#    mu, sigma = 0.2, 0.05
#    mu2, sigma2 = 1, 0.1
#    x1_range = int(seconds/10 * 8)
#    x2_range = seconds - x1_range
#    X1 = [get_value(mu, sigma) for _ in range(x1_range)]
#    X2 = [get_value(mu2, sigma2) for _ in range(x2_range)]
#    X = np.concatenate([X1, X2])
#    return [x for x in X if x <= 1.0]

def binomal_distribution(seconds=900):
    mu, sigma = 0.2, 0.05
    mu2, sigma2 = 0.8, 0.05
    x1_range = int(seconds/10 * 3)
    x2_range = seconds - x1_range
    X1 = [get_value(mu, sigma) for _ in range(x1_range)]
    X2 = [get_value(mu2, sigma2) for _ in range(x2_range + 1)]
    return list(np.concatenate([X1, X2]))

def uniform_distribution(seconds=900):
    return list(np.random.uniform(0.5, 0.55, seconds + 1))

def normal_distribution(seconds=900):
    mu, sigma = 0.3, 0.1
    return [get_value(mu, sigma) for _ in range(seconds + 1)]

if __name__ == '__main__':
    print('hello')