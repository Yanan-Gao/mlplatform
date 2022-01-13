from unittest import TestCase
from plutus.losses import google_mse_loss, google_fpa_nll
import numpy as np
import tensorflow as tf
import tensorflow_probability as tfp

class LossTests(TestCase):
    @staticmethod
    def create_multi_target(mb2w):
        return tf.stack([
            np.ones_like(mb2w, dtype=np.float32),
            np.zeros_like(mb2w, dtype=np.float32),
            np.zeros_like(mb2w, dtype=np.float32),
            mb2w,
            np.zeros_like(mb2w, dtype=np.float32)
        ], axis=1)



    def test_google_fpa_nll(self):
        dist = tfp.distributions.LogNormal(loc=tf.math.log(4.0), scale=0.2)
        mb2w = [2, 3, 4, 5, 6, 7, 8]
        loss = google_fpa_nll(LossTests.create_multi_target(mb2w), dist)
        print(loss)
        print(tf.reduce_mean(loss))

    def test_google_mse_loss(self):
        dist = tfp.distributions.LogNormal(loc=tf.math.log(4.0), scale=0.2)
        mb2w = [2, 3, 4, 5, 6, 7, 8]
        preds = dist.mean()
        loss = google_mse_loss(LossTests.create_multi_target(mb2w), preds)
        print(loss)
        print(tf.reduce_mean(loss))



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
