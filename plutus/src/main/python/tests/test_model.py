import unittest

from plutus.data import tfrecord_dataset, generate_random_pandas
from plutus.features import default_model_features, Feature
from plutus import ModelHeads, CpdType
from plutus.layers import model_input_layer
from plutus.models import dlrm_model, replace_last_layer, basic_model, super_basic_model, fastai_tabular_model
from plutus.features import default_model_features, default_model_targets, get_int_parser
from plutus.losses import google_fpa_nll, google_mse_loss
import tensorflow as tf
import numpy as np
import tempfile
from pathlib import Path


class SimpleDataModelTestCase(unittest.TestCase):

    @classmethod
    def tearDownClass(cls) -> None:
        cls.test_dir.cleanup()

    @classmethod
    def setUpClass(cls) -> None:
        """
        Create some canned training data

        It will create a single feature dataset, where two lognorm distriubtion are used
        to generate artificial market prices.
        """
        NUM_EXAMPLES = 10000

        cls.test_dir = tempfile.TemporaryDirectory()
        cls.model_features = [
            Feature("feat_1", True, tf.int32, 2, 0, True, 16, False, 4),
        ]

        def _float_feature(value):
            return tf.train.Feature(float_list=tf.train.FloatList(value=value))

        def _int64_feature(value):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

        def create_example():
            features = {}
            for f in cls.model_features:
                if f.type == tf.int32:
                    features[f.name] = _int64_feature(np.random.randint(0, f.cardinality, size=1))
                elif f.type == tf.float32:
                    features[f.name] = _float_feature(np.random.random_sample(size=1))

            for t in default_model_targets:
                if t.name == "mb2w":
                    if features['feat_1'].int64_list.value[0] == 0:
                        features[t.name] = _float_feature(np.random.lognormal(np.log(8.), 0.25, 1))
                    else:
                        features[t.name] = _float_feature(np.random.lognormal(np.log(1.), 0.45, 1))
                else:
                    features[t.name] = _float_feature(np.random.random_sample(size=1))

            return tf.train.Example(features=tf.train.Features(feature=features))

        with tf.io.TFRecordWriter(f"{cls.test_dir.name}/aaa.tfrecord.gz", options="GZIP") as writer:
            for i in range(NUM_EXAMPLES):
                example_proto = create_example()
                writer.write(example_proto.SerializeToString())

        p = Path(f"{cls.test_dir.name}")
        files = [p.resolve().__str__() for p in p.iterdir()]
        print(files)
        batch_size = 100

        cls.ds = tfrecord_dataset(files, batch_size, get_int_parser(cls.model_features, default_model_targets))

    def test_something(self):
        print(self.model_features)

        # for x,y in self.ds.unbatch().batch(2).take(10):
        #     print(x)
        #     print(y)

        # model = basic_model(features=self.model_features,
        #                     activation="relu",
        #                     combiner=tf.keras.layers.Flatten(),
        #                     top_mlp_layers=[512, 256, 64],
        #                     cpd_type=CpdType.MIXTURE,
        #                     heads=ModelHeads.CPD_FLOOR,
        #                     mixture_components=3,
        #                     dropout_rate=.2,
        #                     batchnorm=True)

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

        # model = fastai_tabular_model(features=self.model_features,
        #                              activation="relu",
        #                              combiner=tf.keras.layers.Flatten(),
        #                              layers=[512, 256, 64],
        #                              cpd_type=CpdType.LOGNORM,
        #                              heads=ModelHeads.CPD_FLOOR_WIN,
        #                              mixture_components=3,
        #                              dropout_rate=None,
        #                              batchnorm=False)

        model.summary()
        model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
        model.fit(self.ds, epochs=10, verbose=1)

        m2 = tf.keras.Model(model.input, model.get_layer("params").output)

        print(model({'feat_1': tf.constant([0])}, training=False).mean())
        print(m2({'feat_1': tf.constant([0])}, training=False))

        print(model({'feat_1': tf.constant([1])}, training=False).mean())
        print(m2({'feat_1': tf.constant([1])}, training=False))

    # def test_replace_head(self):
    #     model = dlrm_model(default_model_features,
    #                        multi_output=ModelOutput.CPD_FLOOR_WIN,
    #                        cpd_type=CpdType.MIXTURE,
    #                        dropout_rate=0.2,
    #                        batchnorm=True
    #                        )
    #     new_model = replace_last_layer(model)
    #     new_model.summary()
    #
    def test_split_params(self):
        mu = tf.constant(np.arange(30).reshape(10, 3))
        sigma = tf.constant(np.arange(30, 60).reshape(10, 3))
        probs = tf.constant(np.arange(60, 90).reshape(10, 3))

        params = tf.keras.layers.concatenate([mu, sigma, probs])
        print(params)

        a, b, c = tf.split(params, 3, axis=-1)

        tf.assert_equal(mu, a)
        tf.assert_equal(sigma, b)
        tf.assert_equal(probs, c)
        print(a)
        print(b)
        print(c)


class ComplexDataModelTestCase(unittest.TestCase):

    @classmethod
    def tearDownClass(cls) -> None:
        cls.test_dir.cleanup()

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dir = tempfile.TemporaryDirectory()
        cls.model_features = default_model_features

        cls.example = {f.name: tf.constant([np.random.randint(0, f.cardinality, size=1)]) for f in cls.model_features}

        n_observations = 1000

        def _float_feature(value):
            return tf.train.Feature(float_list=tf.train.FloatList(value=value))

        def _int64_feature(value):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

        def create_example():
            features = {}
            for f in cls.model_features:
                if f.type == tf.int32:
                    features[f.name] = _int64_feature(np.random.randint(0, f.cardinality, size=1))
                elif f.type == tf.float32:
                    features[f.name] = _float_feature(np.random.random_sample(size=1))

            for t in default_model_targets:
                features[t.name] = _float_feature(np.random.random_sample(size=1))

            return tf.train.Example(features=tf.train.Features(feature=features))

        with tf.io.TFRecordWriter(f"{cls.test_dir.name}/bbb.tfrecord.gz", options="GZIP") as writer:
            for i in range(n_observations):
                example_proto = create_example()
                writer.write(example_proto.SerializeToString())

        p = Path(f"{cls.test_dir.name}")
        files = [p.resolve().__str__() for p in p.iterdir()]
        batch_size = 100

        cls.ds = tfrecord_dataset(files, batch_size, get_int_parser(cls.model_features, default_model_targets))

    def test_model_training(self):
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
        model.fit(self.ds, epochs=10, verbose=1)

        m2 = tf.keras.Model(model.input, model.get_layer("params").output)

        print(self.example)
        print(model(self.example, training=False).mean())
        print(model(self.example, training=False).cdf(5.5))
        print(model(self.example, training=False).cdf(5.5) * .99)
        print(model(self.example, training=False).quantile(model(self.example, training=False).cdf(5.5) * .99))
        print(model(self.example, training=False).cdf(
            model(self.example, training=False).quantile(model(self.example, training=False).cdf(5.5) * .99)))

        print(m2(self.example, training=False))

        print(model(self.example, training=False).mean())
        print(m2(self.example, training=False))

    def test_comapre_models(self):
        model_dlrm = dlrm_model(features=self.model_features,
                                activation="relu",
                                combiner=tf.keras.layers.Flatten(),
                                top_mlp_layers=[512, 256, 64],
                                bottom_mlp_layers=[512, 256, 64, 16],
                                cpd_type=CpdType.LOGNORM,
                                heads=ModelHeads.CPD,
                                mixture_components=3,
                                dropout_rate=None,
                                batchnorm=True)

        model_fastai = fastai_tabular_model(features=self.model_features,
                                            activation="relu",
                                            combiner=tf.keras.layers.Flatten(),
                                            layers=[1000, 500],
                                            cpd_type=CpdType.LOGNORM,
                                            heads=ModelHeads.CPD,
                                            mixture_components=3,
                                            dropout_rate=None,
                                            batchnorm=True)

        ds_tf = tfrecord_dataset(["/Users/michael.davy/Downloads/train.tfrecord.gz"], 2**13, get_int_parser(self.model_features, default_model_targets))

        model_dlrm.compile(optimizer=tf.keras.optimizers.Adam(), loss=google_fpa_nll)
        model_fastai.compile(optimizer=tf.keras.optimizers.Adam(), loss=google_fpa_nll)

        h_dlrm = model_dlrm.fit(ds_tf)
        h_fastai = model_fastai.fit(ds_tf)

        for x,y in ds_tf.take(1):
            pred_dlrm= model_dlrm(x, training=False)
            print(google_fpa_nll(y, pred_dlrm))

            pred_fastai = model_fastai(x, training=False)
            print(google_fpa_nll(y, pred_fastai))


            print("done")



if __name__ == '__main__':
    unittest.main()
