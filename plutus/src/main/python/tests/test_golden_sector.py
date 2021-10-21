from unittest import TestCase
import numpy as np
import tensorflow as tf
import tensorflow_probability as tfp

def golden_sector(bid_price, dist, floor=0.0, epsilon=0.1, max_num_iter=100):
    gr = (np.sqrt(5) + 1) / 2

    def surplus(x):
        return ((bid_price - x) * dist.cdf(x))

    b_min, b_max = floor, bid_price
    x_1 = b_max - (b_max - b_min ) /gr
    x_2 = b_min + (b_max - b_min ) /gr

    for i in range(max_num_iter):
        if surplus(x_1) > surplus(x_2):
            b_max = x_2
        else:
            b_min = x_1

        if (b_max - b_min) < epsilon:
            return (b_min + b_max) / 2

        x_1 = b_max - (b_max - b_min ) /gr
        x_2 = b_min + (b_max - b_min ) /gr

    return (b_min + b_max) / 2

class LossTests(TestCase):
    def test_golden_sector(self):

        d = tfp.distributions.Normal(2.4, 0.1)


        print(golden_sector(4.2, d))