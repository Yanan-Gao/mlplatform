from unittest import TestCase

import tensorflow as tf

from plutus import CpdType, ModelHeads
from plutus.data import generate_random_pandas
from plutus.features import default_model_features, default_model_targets
from plutus.losses import google_fpa_nll
from plutus.metrics import evaluate_model_saving
from plutus.models import dlrm_model


class TestMetrics(TestCase):

    def setUp(self) -> None:
        self.model_features = default_model_features
        self.model_targets = default_model_targets
        self.n_observations = 1000
        self.batch_size = 10
        self.num_epochs = 2

        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        self.ds = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)

        self.model = dlrm_model(features=self.model_features,
                                activation="relu",
                                combiner=tf.keras.layers.Flatten(),
                                top_mlp_layers=[512, 256, 64],
                                bottom_mlp_layers=[512, 256, 64, 16],
                                cpd_type=CpdType.LOGNORM,
                                heads=ModelHeads.CPD,
                                mixture_components=3,
                                dropout_rate=None,
                                batchnorm=False)

        self.model.summary()

        self.model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
        self.model.fit(self.ds, epochs=self.num_epochs, verbose=1)

    def test_evaluate_model_saving(self):
        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        ds_test = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)

        results_df = evaluate_model_saving(self.model,
                                           ds_test,
                                           self.batch_size)

        print(results_df)
