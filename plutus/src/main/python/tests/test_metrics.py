from unittest import TestCase

import tensorflow as tf
import tensorflow_probability as tfp

import plutus.metrics
from plutus import CpdType, ModelHeads
from plutus.data import generate_random_pandas
from plutus.features import default_model_features, default_model_targets
from plutus.losses import google_fpa_nll
from plutus.metrics import evaluate_model_saving
from plutus.models import dlrm_model


class TestMetrics(TestCase):

    # def setUp(self) -> None:
    #     self.model_features = default_model_features
    #     self.model_targets = default_model_targets
    #     self.n_observations = 1000
    #     self.batch_size = 10
    #     self.num_epochs = 2
    #
    #     features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
    #     self.ds = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)
    #
    #     self.model = dlrm_model(features=self.model_features,
    #                             activation="relu",
    #                             combiner=tf.keras.layers.Flatten(),
    #                             top_mlp_layers=[512, 256, 64],
    #                             bottom_mlp_layers=[512, 256, 64, 16],
    #                             cpd_type=CpdType.LOGNORM,
    #                             heads=ModelHeads.CPD,
    #                             mixture_components=3,
    #                             dropout_rate=None,
    #                             batchnorm=False)
    #
    #     self.model.summary()
    #
    #     self.model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
    #     self.model.fit(self.ds, epochs=self.num_epochs, verbose=1)

    def test_evaluate_model_saving(self):
        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        ds_test = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)

        results_df = evaluate_model_saving(self.model,
                                           ds_test,
                                           self.batch_size)

        print(results_df)


    def test_anlp(self):
        anlp = plutus.metrics.ANLP()

        dist = tfp.distributions.LogNormal(loc=tf.math.log(4.0), scale=0.2)
        # # print(tf.math.log(4.0))
        # # print(dist.mean())
        # print(f"{dist.prob(1.0)=}\t{dist.log_prob(1.0)=}")
        # print(f"{dist.prob(2.0)=}\t{dist.log_prob(2.0)=}")
        # print(f"{dist.prob(3.0)=}\t{dist.log_prob(3.0)=}")
        # print(f"{dist.prob(4.0)=}\t{dist.log_prob(4.0)=}")
        # print(f"{dist.prob(5.0)=}\t{dist.log_prob(5.0)=}")
        # print(f"{dist.prob(6.0)=}\t{dist.log_prob(6.0)=}")

        # close
        close_values = [3., 4., 5.]
        expected_close_result = -tf.reduce_mean(dist.log_prob(close_values))
        anlp.update_state(y_true=close_values, y_pred=dist)
        self.assertAlmostEqual(expected_close_result.numpy(), anlp.result().numpy(), 7)

        # Not close (small)
        anlp.reset_states()
        small_values = [.3, .4, .5]
        expected_small_value_result = -tf.reduce_mean(dist.log_prob(small_values))
        anlp.update_state(y_true=small_values, y_pred=dist)
        self.assertAlmostEqual(expected_small_value_result.numpy(), anlp.result().numpy(), 7)

        # Not close (big)
        anlp.reset_states()
        large_values = [30., 40., 50.]
        expected_large_value_result = -tf.reduce_mean(dist.log_prob(large_values))
        anlp.update_state(y_true=large_values, y_pred=dist)
        self.assertAlmostEqual(expected_large_value_result.numpy(), anlp.result().numpy(), 7)

        ## Multiple batches as input
        anlp.reset_states()
        batch_values = [30., 40., 50., 1., .1, .5, .3, .01, 1., .1, .5, .3, .01]

        expected_batch_result = -tf.reduce_mean(dist.log_prob(batch_values))
        anlp.update_state(y_true=batch_values[:len(batch_values)//2], y_pred=dist)
        anlp.update_state(y_true=batch_values[len(batch_values)//2:], y_pred=dist)
        # print(tf.reduce_mean(dist.log_prob(batch_values[:len(batch_values)//2])))
        # print(tf.reduce_mean(dist.log_prob(batch_values[len(batch_values)//2:])))
        # print((tf.reduce_mean(dist.log_prob(batch_values[:len(batch_values) // 2])) + tf.reduce_mean(dist.log_prob(batch_values[len(batch_values) // 2:]))) /2)
        # print(expected_batch_result)
        # print(anlp.result())
        self.assertAlmostEqual(expected_batch_result.numpy(), anlp.result().numpy(), 7)

        x = tf.constant([
            [1],
            [3]
        ])
        print(x.shape[-1])


    def test_anlp(self):
        anlp = plutus.metrics.ANLP()

        dist = tfp.distributions.LogNormal(loc=tf.math.log(4.0), scale=0.2)
        # # print(tf.math.log(4.0))
        # # print(dist.mean())
        # print(f"{dist.prob(1.0)=}\t{dist.log_prob(1.0)=}")
        # print(f"{dist.prob(2.0)=}\t{dist.log_prob(2.0)=}")
        # print(f"{dist.prob(3.0)=}\t{dist.log_prob(3.0)=}")
        # print(f"{dist.prob(4.0)=}\t{dist.log_prob(4.0)=}")
        # print(f"{dist.prob(5.0)=}\t{dist.log_prob(5.0)=}")
        # print(f"{dist.prob(6.0)=}\t{dist.log_prob(6.0)=}")

        # close
        close_values = [3., 4., 5.]
        expected_close_result = -tf.reduce_mean(dist.log_prob(close_values))
        anlp.update_state(y_true=close_values, y_pred=dist)
        self.assertAlmostEqual(expected_close_result.numpy(), anlp.result().numpy(), 7)

        # Not close (small)
        anlp.reset_states()
        small_values = [.3, .4, .5]
        expected_small_value_result = -tf.reduce_mean(dist.log_prob(small_values))
        anlp.update_state(y_true=small_values, y_pred=dist)
        self.assertAlmostEqual(expected_small_value_result.numpy(), anlp.result().numpy(), 7)

        # Not close (big)
        anlp.reset_states()
        large_values = [30., 40., 50.]
        expected_large_value_result = -tf.reduce_mean(dist.log_prob(large_values))
        anlp.update_state(y_true=large_values, y_pred=dist)
        self.assertAlmostEqual(expected_large_value_result.numpy(), anlp.result().numpy(), 7)

        ## Multiple batches as input
        anlp.reset_states()
        batch_values = [30., 40., 50., 1., .1, .5, .3, .01, 1., .1, .5, .3, .01]

        expected_batch_result = -tf.reduce_mean(dist.log_prob(batch_values))
        anlp.update_state(y_true=batch_values[:len(batch_values)//2], y_pred=dist)
        anlp.update_state(y_true=batch_values[len(batch_values)//2:], y_pred=dist)
        # print(tf.reduce_mean(dist.log_prob(batch_values[:len(batch_values)//2])))
        # print(tf.reduce_mean(dist.log_prob(batch_values[len(batch_values)//2:])))
        # print((tf.reduce_mean(dist.log_prob(batch_values[:len(batch_values) // 2])) + tf.reduce_mean(dist.log_prob(batch_values[len(batch_values) // 2:]))) /2)
        # print(expected_batch_result)
        # print(anlp.result())
        self.assertAlmostEqual(expected_batch_result.numpy(), anlp.result().numpy(), 7)

        x = tf.constant([
            [1],
            [3]
        ])
        print(x.shape[-1])
