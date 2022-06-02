import os
from typing import List

import tensorflow as tf
from pathlib import Path
import math
import pandas as pd
import numpy as np
from tensorflow_serving.apis import predict_pb2

from philo.features import get_map_function, get_map_function_test

TRAIN = "train"
VAL = "validation"
TEST = "test"


def s3_sync(src_path, dst_path):
    sync_command = f"aws s3 sync {src_path} {dst_path}"
    os.system(sync_command)
    return sync_command


# def read_metadata(path):
#     p = Path(f"{path}")
#     files = [str(f.resolve()) for f in list(p.glob("*.csv"))]
#     return pd.read_csv(files[0], delimiter="\t")

# def get_epochs(path, batch_size, steps_per_epoch):
#     df = read_metadata(path)
#     batch_per_dataset = df['train'].iloc[0] // batch_size
#     epochs = batch_per_dataset // steps_per_epoch
#     return epochs


def list_tfrecord_files(path, format=None):
    """create a dictionary with each key represents a list of corresponding
       tfrecord files

    Args:
        path (string): path to the tfrecord files
        format (str): gzip or tf

    Returns:
        dict: dictionary for train validate test
    """
    p = Path(f"{path}")
    if format == "gzip":
        glob_str = "*.gz"
    else:
        glob_str = "part*"
    files = [str(f.resolve()) for f in list(p.glob(glob_str))]
    if len(files) == 0:
        files_dir = {x.name: [str(f.resolve()) for f in list(Path(f"{path}/{x.name}/").glob(glob_str))] for x in
                     p.iterdir()
                     if x.is_dir()}
    else:
        files_dir = {"train": files}
    return files_dir


def tfrecord_dataset(files, batch_size, map_fn, compression_type=None, prefetch_num=10):
    """
    create tf data pipeline
    Args:
        files:  list of files
        batch_size: batch size
        map_fn: transformation function
        compression_type: type of compression
                          None (if data is produced without compression)
                          or GZIP (if data is produced through pipeline)
        prefetch_num: prefetch batches

    Returns: tf data pipeline

    """
    return tf.data.TFRecordDataset(
        files,
        compression_type=compression_type,
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


#
#
# def downsample(files_dict, down_sample_rate=None):
#     if down_sample_rate is not None:
#         files_dict_sample = {}
#         for k, v in files_dict.items():
#             files_dict_sample[k] = files_dict[k][: math.ceil(len(v) * down_sample_rate)]
#         return files_dict_sample
#
#     return files_dict
#
#
def create_datasets(files_dict, batch_size, model_features,
                    model_target, map_function=get_map_function,
                    map_function_test=get_map_function_test,
                    compression_type=None, eval_batch_size=None,
                    test_batch_size=2 ** 17, offline_test=False):
    """
    create tf data
    Args:
        files_dict: dictionary of train validate and test
        batch_size: batch size for train
        model_features: list of features
        model_target: model target
        map_function: transformation function
        map_function_test: transformation function during offline testing
        compression_type: type of tfrecords compression format
        eval_batch_size: batch size for evaluation
        test_batch_size: batch size for testing
        offline_test: do offline test or not, this decides whether test data will generate bidrequestid

    Returns:

    """
    ds = {}
    for split, files in files_dict.items():
        if split == TRAIN:
            ds[split] = tfrecord_dataset(files,
                                         batch_size,
                                         map_function(model_features, model_target), compression_type
                                         )
        elif split == TEST:
            if offline_test:
                ds[split] = tfrecord_dataset(files,
                                             test_batch_size if test_batch_size is not None else batch_size,
                                             map_function_test(model_features, model_target, "BidRequestId"),
                                             compression_type
                                             )
            else:
                ds[split] = tfrecord_dataset(files,
                                             eval_batch_size if eval_batch_size is not None else batch_size,
                                             map_function(model_features, model_target), compression_type
                                             )
        else:
            ds[split] = tfrecord_dataset(files,
                                         eval_batch_size if eval_batch_size is not None else batch_size,
                                         map_function(model_features, model_target), compression_type
                                         )
    return ds


#
#
# # def calc_epocs_per_data():
# #     batch_per_epoch = (num_train_examples // batch_size) // epoch_per_dataset
# #
# #     print(f"{num_train_examples=:>20,}")
# #     print(f"{num_val_examples=:>20,}")
# #     print(f"{num_test_examples=:>20,}")
# #
# #     print(f"{batch_size=:>20,}")
# #     print(f"{epoch_per_dataset=:>20,}")
# #     print(f"{batch_per_epoch=:>20,}")
#
#
# def _float_feature(value):
#     return tf.train.Feature(float_list=tf.train.FloatList(value=value))
#
#
# def _int64_feature(value):
#     return tf.train.Feature(int64_list=tf.train.Int64List(value=value))
#
#
# def generate_random_tf_example(model_features, model_targets):
#     features = {}
#     for f in model_features:
#         if f.type == tf.int32:
#             features[f.name] = _int64_feature(np.random.randint(0, f.cardinality, size=1))
#
#         elif f.type == tf.float32:
#             features[f.name] = _float_feature(np.random.random_sample(size=1))
#     for t in model_targets:
#         features[t.name] = _float_feature(np.random.random_sample(size=1))
#
#     return tf.train.Example(features=tf.train.Features(feature=features))
#
#
def generate_random_pandas(model_features, model_target, num_examples=1000):
    """
    generate artificial pandas data
    Args:
        model_features: list of model features
        model_target: model target
        num_examples: number of samples

    Returns: artificial data

    """
    features = {}
    for f in model_features:
        if f.sparse:
            features[f.name] = np.random.randint(0, f.cardinality, size=num_examples)

        else:
            features[f.name] = np.random.random_sample(size=num_examples).astype(np.float32)

    if model_target.binary:
        targets = np.random.binomial(n=1, p=0.01, size=num_examples).astype(np.float32)
    else:
        targets = np.random.random_sample(size=num_examples).astype(np.float32)

    return pd.DataFrame(features), targets


def prepare_dummy_data(model_features, model_target, batch_size):
    """
    Used for testing. This creates a dummy dataset from random data.
    """

    def generate_tensor_slices(num):
        x, y = generate_random_pandas(model_features, model_target, num)
        return dict(x), y

    return {
        TRAIN: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(1000)).batch(batch_size),
        VAL: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).batch(batch_size),
        TEST: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).batch(batch_size)
    }


def prepare_real_data(model_features, model_target, input_path, batch_size, eval_batch_size, offline_test=False):
    """
    create real dataset
    Args:
        model_features: list of model feature settings
        model_target: model target setting
        input_path: input path for data
        batch_size: batch size for training
        eval_batch_size: batch size for evaluating
        offline_test: if true, generate bidrequest id besides features and target

    Returns:

    """
    print(input_path)
    files = list_tfrecord_files(input_path)
    print(files)
    return create_datasets(files_dict=files, batch_size=batch_size, model_features=model_features,
                           model_target=model_target, compression_type="GZIP", eval_batch_size=eval_batch_size,
                           offline_test=offline_test)

def generate_random_grpc_query(model_features, model_name, version=None):
    """

    Args:
        model_features: list of Feature nametuple
        model_name: name of the model
        version: version of the model

    Returns: tf serving rest api request

    """
    grpc_request = predict_pb2.PredictRequest()
    grpc_request.model_spec.name = model_name
    grpc_request.model_spec.signature_name = 'serving_default'
    if version is not None:
        grpc_request.model_spec.version_label = version

    for f in model_features:
        if f.type == tf.int32:
            grpc_request.inputs[f.name].CopyFrom(
                tf.make_tensor_proto(np.random.randint(0, f.cardinality, size=1).astype(np.int32).reshape(-1, 1)))

        elif f.type == tf.float32:
            grpc_request.inputs[f.name].CopyFrom(tf.make_tensor_proto([[0.0]]))

    return grpc_request


