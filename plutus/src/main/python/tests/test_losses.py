from unittest import TestCase
from plutus.losses import google_mse_loss
import numpy as np
import tensorflow as tf

class LossTests(TestCase):
    def test_censored_fpa_nll(self):
        y = tf.stack([[1.],
                      [0.5],
                      [0.5],
                      [0.4],
                      [0.1],
                      ], axis=1)


        # _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)

        pred = 0.5
        result = google_mse_loss(y, pred)
        print(result)
        self.assertAlmostEqual(result.numpy(), 0.01)
