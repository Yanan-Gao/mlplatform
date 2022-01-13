import unittest
from plutus.features import default_model_features, default_model_targets, get_model_features, get_int_parser
from plutus.data import tfrecord_dataset
import tensorflow as tf
import numpy as np
import tempfile
from pathlib import Path


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.test_dir.cleanup()

    def test_get_model_features(self):
        features = get_model_features()
        self.assertListEqual(features, default_model_features)

        features = get_model_features(["DealId"])
        self.assertNotEqual(features, default_model_features)

    def test_create_tfrecord_data(self):
        def _bytes_feature(value):
            if isinstance(value, type(tf.constant(0))):
                value = value.numpy()  # BytesList won't unpack a string from an EagerTensor.
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=value))

        def _float_feature(value):
            return tf.train.Feature(float_list=tf.train.FloatList(value=value))

        def _int64_feature(value):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

        def create_example():
            features={}
            for f in default_model_features:
                if f.type == tf.int32:
                    features[f.name] = _int64_feature(np.random.randint(0, f.cardinality, size=1))
                elif f.type == tf.float32:
                    features[f.name] = _float_feature(np.random.random_sample(size=1))

            for t in default_model_targets:
                features[t.name] = _float_feature(np.random.random_sample(size=1))

            return tf.train.Example(features=tf.train.Features(feature=features))

        print(self.test_dir)

        n_observations = 1000
        with tf.io.TFRecordWriter(f"{self.test_dir.name}/aaa.tfrecord.gz", options="GZIP") as writer:
            for i in range(n_observations):
                example_proto = create_example()
                writer.write(example_proto.SerializeToString())

        p = Path(f"{self.test_dir.name}")
        print([f"{p} - {p.stat().st_size}" for p in p.iterdir()])

    def test_read_tfrecord(self):
        p = Path(f"{self.test_dir.name}")
        files = [p.resolve().__str__() for p in p.iterdir()]
        print(files)
        batch_size = 1

        get_int_parser(default_model_features, default_model_targets)
        ds = tfrecord_dataset(files, batch_size, get_int_parser(default_model_features, default_model_targets))
        for x, y in ds.take(100):
            print(x)
            print(y)

        # TODO: create some tests here


if __name__ == '__main__':
    unittest.main()
