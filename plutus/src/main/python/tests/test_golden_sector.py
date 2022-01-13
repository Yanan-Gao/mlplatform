from unittest import TestCase
import numpy as np
import tensorflow as tf
import tensorflow_probability as tfp

from plutus import CpdType, ModelHeads
from plutus.data import generate_random_pandas
from plutus.dist import lognorm_distr
from plutus.features import default_model_features, default_model_targets
from plutus.layers import GssLogNormLayer
from plutus.losses import google_fpa_nll
from plutus.models import dlrm_model
from math import sqrt

golden_ratio = (sqrt(5.) + 1) / 2


def gss_fn(bid_price, dist_params, floor=0.0, epsilon=0.1, max_num_iter=100):
    dist = lognorm_distr(dist_params)

    def surplus(bid_price, discount):
        return (bid_price - (discount * bid_price)) * dist.cdf(discount * bid_price)

    b_min, b_max = floor, 1.0
    x_1 = b_max - (b_max - b_min) / golden_ratio
    x_2 = b_min + (b_max - b_min) / golden_ratio

    for i in range(max_num_iter):
        if surplus(bid_price, x_1) > surplus(bid_price, x_2):
            b_max = x_2
        else:
            b_min = x_1

        if (b_max - b_min) < epsilon:
            return (b_min + b_max) / 2

        x_1 = b_max - (b_max - b_min) / golden_ratio
        x_2 = b_min + (b_max - b_min) / golden_ratio

    return (b_min + b_max) / 2

class LossTests(TestCase):

    def test_gss_with_model(self):
        self.model_features = default_model_features
        self.model_targets = default_model_targets
        self.n_observations = 1000
        self.batch_size = 10
        self.num_epochs = 2

        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        self.ds = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)

        model = dlrm_model(features=self.model_features,
                           activation="relu",
                           combiner=tf.keras.layers.Flatten(),
                           top_mlp_layers=[512, 256, 64],
                           bottom_mlp_layers=[512, 256, 64, 16],
                           cpd_type=CpdType.LOGNORM,
                           heads=ModelHeads.CPD,
                           mixture_components=3,
                           dropout_rate=None,
                           batchnorm=False)

        model.summary()


        model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
        model.fit(self.ds, epochs=self.num_epochs, verbose=1)

        return model


    def test_input(self):
        self.model_features = default_model_features
        self.model_targets = default_model_targets
        self.n_observations = 1000
        self.batch_size = 10
        self.num_epochs = 2

        def get_param_model(model_features=default_model_features,
                            model_targets=default_model_targets,
                            num_obs=1000,
                            batch_size=10,
                            epochs=2,
                            compile=False):

            features, targets = generate_random_pandas(model_features, model_targets, num_obs)
            self.ds = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(batch_size)

            model = dlrm_model(features=self.model_features,
                               activation="relu",
                               combiner=tf.keras.layers.Flatten(),
                               top_mlp_layers=[512, 256, 64],
                               bottom_mlp_layers=[512, 256, 64, 16],
                               cpd_type=CpdType.LOGNORM,
                               heads=ModelHeads.CPD,
                               mixture_components=3,
                               dropout_rate=None,
                               batchnorm=False)

            # model.summary()

            if compile:
                model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
                model.fit(self.ds, epochs=epochs, verbose=1)

            return tf.keras.Model(model.inputs, model.get_layer("params").output)

        model_param = get_param_model()


        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        ds_feat = tf.data.Dataset.from_tensor_slices((dict(features))).batch(self.batch_size)
        ds_bid = tf.data.Dataset.from_tensor_slices((targets.iloc[:, 1])).batch(self.batch_size)
        ds_label = tf.data.Dataset.from_tensor_slices((targets)).batch(self.batch_size)
        self.ds = tf.data.Dataset.zip(((ds_feat, ds_bid), ds_label))



        # how long to just iterate over the results?
        # Way too long! (500K in 24h)

        # for x, y in self.ds.unbatch().batch(3).take(10):
        #     bid_req_features, bid_price = x
        #     pred = model_param(bid_req_features, training=False)
        #
        #     bp = bid_price.numpy()[0]
        #     dp = pred.numpy()[0]
        #     mb2w = y.numpy()[:, 3][0]
        #     print(f"{bp=}")
        #     print(f"{dp=}")
        #     print(f"{mb2w=}")
        #
        #     gss_rate = gss_fn(
        #         bid_price=bp,
        #         dist_params=dp,
        #     )
        #
        #     print(f"{gss_rate=}")
        #     print(f"{bp*gss_rate=}")

        def get_gss_model(model_param):
            i_model = model_param.inputs
            print(i_model)
            i_bp = tf.keras.layers.Input(shape=(1,), dtype=tf.float32, name="bid_price")

            # params = tf.expand_dims(model_param(i_model), axis=-1)
            params = model_param(i_model)
            # gss_in = tf.keras.layers.concatenate([params, i_bp])

            gss = GssLogNormLayer()
            gss_out = gss(params, i_bp)

            return tf.keras.Model(i_model, gss_out)

        model_gss = get_gss_model(model_param)
        model_gss.summary()

        for x, y in self.ds.unbatch().batch(3).take(1):
            bid_req_features, bid_price = x
            print(bid_req_features)
            print(bid_price)
            params = model_param(bid_req_features, training=False)
            print(params)

            bid_req_features["bid_price"] = bid_price
            p = model_gss(bid_req_features, training=False)
            # print(model_param.inputs)
            print(p)

    def test_golden_sector(self):

        d = tfp.distributions.LogNormal(tf.math.log(2.4), 0.1)
        print(f"{d.cdf(4.2)}")

        for i in np.arange(0.1, 0.9, 0.1):
            pd = gss_fn(4.2, [tf.math.log(2.4), i])
            print(f"{pd=} {i=} {4.2*pd}")