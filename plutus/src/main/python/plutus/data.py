import os
from typing import List

import tensorflow as tf
import tensorflow_probability as tfp
from pathlib import Path
import math
import pandas as pd
import numpy as np
from tensorflow_serving.apis import predict_pb2

from plutus.features import get_int_parser

tfd = tfp.distributions
tfb = tfp.bijectors

TRAIN = "train"
VAL = "validation"
TEST = "test"


def s3_sync(src_path, dst_path):
    sync_command = f"aws s3 sync {src_path} {dst_path}"
    os.system(sync_command)
    return sync_command


def read_metadata(path):
    p = Path(f"{path}")
    files = [str(f.resolve()) for f in list(p.glob("*.csv"))]
    return pd.read_csv(files[0], delimiter="\t")


def get_epochs(path, batch_size, steps_per_epoch):
    df = read_metadata(path)
    batch_per_dataset = df['train'].iloc[0] // batch_size
    epochs = batch_per_dataset // steps_per_epoch
    return epochs


def list_tfrecord_files(path):
    p = Path(f"{path}")
    files = [str(f.resolve()) for f in list(p.glob("*.gz"))]
    if len(files) == 0:
        files_dir = {x.name: [str(f.resolve()) for f in list(Path(f"{path}/{x.name}/").glob("*.gz"))] for x in
                     p.iterdir()
                     if x.is_dir()}
    else:
        files_dir = {"train": files}
    return files_dir


def tfrecord_dataset(files, batch_size, map_fn, prefetch_num=10):
    return tf.data.TFRecordDataset(
        files,
        compression_type="GZIP",
    ).batch(
        batch_size=batch_size,
        drop_remainder=True
    ).map(
        map_fn,
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False
    ).prefetch(
        prefetch_num
    )


def downsample(files_dict, down_sample_rate=None):
    if down_sample_rate is not None:
        files_dict_sample = {}
        for k, v in files_dict.items():
            files_dict_sample[k] = files_dict[k][: math.ceil(len(v) * down_sample_rate)]
        return files_dict_sample

    return files_dict


def datasets(files_dict, batch_size, model_features, model_targets, eval_batch_size=None):
    ds = {}
    for split, files in files_dict.items():
        if split != TRAIN:
            ds[split] = tfrecord_dataset(files,
                                         eval_batch_size if eval_batch_size is not None else batch_size,
                                         get_int_parser(model_features, model_targets)
                                         )
        else:
            ds[split] = tfrecord_dataset(files,
                                         batch_size,
                                         get_int_parser(model_features, model_targets)
                                         )
    return ds


def _float_feature(value):
    return tf.train.Feature(float_list=tf.train.FloatList(value=value))


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=value))


def generate_random_tf_example(model_features, model_targets):
    features = {}
    for f in model_features:
        if f.type == tf.int32:
            features[f.name] = _int64_feature(np.random.randint(0, f.cardinality, size=1))

        elif f.type == tf.float32:
            features[f.name] = _float_feature(np.random.random_sample(size=1))
    for t in model_targets:
        features[t.name] = _float_feature(np.random.random_sample(size=1))

    return tf.train.Example(features=tf.train.Features(feature=features))


def generate_random_pandas(model_features, model_targets, num_examples=1000):
    features = {}
    for f in model_features:
        if f.type == tf.int32:
            features[f.name] = np.random.randint(0, f.cardinality, size=num_examples)

        elif f.type == tf.float32:
            features[f.name] = np.random.random_sample(size=num_examples).astype(np.float32)

    targets = {}
    for t in model_targets:
        if t.binary:
            targets[t.name] = np.random.binomial(n=1, p=0.3, size=num_examples).astype(np.float32)
        else:
            targets[t.name] = np.random.random_sample(size=num_examples).astype(np.float32)

    return pd.DataFrame(features), pd.DataFrame(targets)


def generate_random_grpc_query(model_features, model_name, version=None):
    grpc_request = predict_pb2.PredictRequest()
    grpc_request.model_spec.name = model_name
    grpc_request.model_spec.signature_name = 'serving_default'
    if version is not None:
        grpc_request.model_spec.version_label = version

    for f in model_features:
        if f.type == tf.int32:
            grpc_request.inputs[f.name].CopyFrom(tf.make_tensor_proto(np.random.randint(0, f.cardinality, size=1).astype(np.int32)))

        elif f.type == tf.float32:
            grpc_request.inputs[f.name].CopyFrom(tf.make_tensor_proto([[0.0]]))

    return grpc_request
