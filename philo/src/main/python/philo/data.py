import os
from typing import List
from datetime import timedelta

import tensorflow as tf
from pathlib import Path
import math
import pandas as pd
import numpy as np
from tensorflow_serving.apis import predict_pb2

from philo.features import get_map_function, get_map_function_test, get_map_function_weighted

TRAIN = "train"
VAL = "validation"
TEST = "test"


def s3_sync(src_path, dst_path):
    """
    Sync data from source path to destination path, need awscli
    Args:
        src_path: source path
        dst_path: destination path

    Returns: command for syncing

    """
    sync_command = f"aws s3 sync {src_path} {dst_path}"
    os.system(sync_command)
    return sync_command


def read_metadata(file_names):
    """
    Read metadata from metadata path
    Args:
        file_names: list of file names for metadata

    Returns: dataframe with the count for each class

    """
    all_data = pd.concat([pd.read_csv(f) for f in file_names])
    return all_data.groupby("label").sum()


def get_meta_files_emr(path):
    """
    Read metadata from metadata path in emr
    Args:
        path: path to the metadata

    Returns: list of meta file names

    """
    p = Path(path)
    files = [str(f.resolve()) for f in list(p.glob("*.csv"))]
    return files


def get_steps_epochs(metadata, batch_size, trunks, multiplier=5):
    """
    Calculate steps_per_epochs and number of epochs
    Args:
        metadata: metadata df, has count for each class
        batch_size: batch size
        trunks: number of trunks that the whole dataset shall be divided into
        multiplier: how many loops of total data should the algorithm go through

    Returns: steps_per_epoch and number of epochs

    """
    total_n = metadata.sum()["count"]
    samples_per_epoch = total_n // trunks
    steps_per_epoch = samples_per_epoch // batch_size
    return steps_per_epoch, trunks * multiplier


def get_steps_epochs_emr(path, batch_size, trunks, multiplier=5):
    """
    get steps epoch info for automatic run on emr
    Args:
        path: path to the metadata files
        batch_size: batch size for each step
        trunks: number of trunks the data is divided into
        multiplier: trunk * multiplier is the total number of epochs

    Returns: steps_per_epoch, epochs

    """
    if trunks == 1:
        return None, multiplier
    files = get_meta_files_emr(path)
    metadata = read_metadata(files)
    steps_per_epoch, epochs = get_steps_epochs(metadata=metadata, batch_size=batch_size,
                                               trunks=trunks, multiplier=multiplier)
    return steps_per_epoch, epochs


def list_tfrecord_files(path, data_format="GZIP"):
    """create a dictionary with each key represents a list of corresponding
       tfrecord files

    Args:
        path (string): path to the tfrecord files
        data_format (str): gzip or not zipped

    Returns:
        dict: dictionary for train validate test
    """
    p = Path(f"{path}")
    if data_format == "GZIP":
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


def tfrecord_dataset(files, batch_size, map_fn, compression_type="GZIP", prefetch_num=10, repeat=False):
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
        repeat: whether repeat dataset or not, useful if run through data set in multiple epochs, need to set the
                steps_per_epochs if repeat is true

    Returns: tf data pipeline

    """
    data = tf.data.TFRecordDataset(files, compression_type=compression_type)
    if repeat:
        data = data.repeat()

    return data.batch(
        batch_size=batch_size,
        drop_remainder=True
    ).map(
        map_func=map_fn,
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False
    ).prefetch(
        prefetch_num
    )


def create_datasets(files_dict, batch_size, model_features,
                    model_target, map_function,
                    compression_type="GZIP", eval_batch_size=None,
                    prefetch_num=10, test_batch_size=2 ** 17,
                    offline_test=False, repeat=False):
    """
    create tf data
    Args:
        prefetch_num: prefetch for tf.data to load data to cpu
        files_dict: dictionary of train validate and test
        batch_size: batch size for train
        model_features: list of features
        model_target: model target
        map_function: transformation function
        compression_type: type of tfrecords compression format
        eval_batch_size: batch size for evaluation
        test_batch_size: batch size for testing
        offline_test: do offline test or not, this decides whether test data will generate bidRequestId
        repeat: whether repeat dataset or not, useful if run through data set in multiple epochs, need to set the
                steps_per_epochs if repeat is true
    Returns: dictionary of datasets

    """
    ds = {}
    for split, files in files_dict.items():
        if split == TRAIN:
            ds[split] = tfrecord_dataset(files=files,
                                         batch_size=batch_size,
                                         map_fn=map_function,
                                         compression_type=compression_type,
                                         prefetch_num=prefetch_num, repeat=repeat
                                         )
        elif split == TEST:
            if offline_test:
                map_test_func = get_map_function_test(model_features, model_target, "BidRequestId")
                ds[split] = tfrecord_dataset(files=files,
                                             batch_size=test_batch_size if test_batch_size is not None else batch_size,
                                             map_fn=map_test_func,
                                             compression_type=compression_type
                                             )
            else:
                ds[split] = tfrecord_dataset(files=files,
                                             batch_size=eval_batch_size if eval_batch_size is not None else batch_size,
                                             map_fn=map_function,
                                             compression_type=compression_type
                                             )
        else:
            ds[split] = tfrecord_dataset(files=files,
                                         batch_size=eval_batch_size if eval_batch_size is not None else batch_size,
                                         map_fn=map_function,
                                         compression_type=compression_type
                                         )
    return ds


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


def prepare_real_data(model_features, model_target, input_path,
                      batch_size, eval_batch_size, prefetch_num=10,
                      offline_test=False, repeat=False, weight_name=None,
                      weight_value=None):
    """
    create real dataset
    Args:
        prefetch_num: prefetch data to cpu memory
        model_features: list of model feature settings
        model_target: model target setting
        input_path: input path for data
        batch_size: batch size for training
        eval_batch_size: batch size for evaluating
        offline_test: if true, generate bidRequestId besides features and target
        repeat: if data is split into trunks, need to set it to True
        weight_name: name for the weight column
        weight_value: weight value

    Returns: dictionary of dataset

    """
    # print(input_path)
    files = list_tfrecord_files(input_path)
    # print(files)
    if weight_name is not None and weight_value is not None:
        map_func = get_map_function_weighted(model_features, model_target, weight_name, weight_value)
    elif weight_name is None and weight_value is None:
        map_func = get_map_function(model_features, model_target)
    else:
        raise Exception('weight_name and weight have to be both not None to use the sample_weight')
    return create_datasets(files_dict=files, batch_size=batch_size, model_features=model_features,
                           model_target=model_target, map_function=map_func, compression_type="GZIP",
                           eval_batch_size=eval_batch_size, prefetch_num=prefetch_num, offline_test=offline_test,
                           repeat=repeat)


def generate_random_grpc_query(model_features, model_name, version=None):
    """

    Args:
        model_features: list of Feature name tuple
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


def get_file_list_between_date(sdate, edate, input_path, suffix="gz"):
    """Find tf files between the date that is used
       only used in adhoc operation, this is used
       in databricks environment, the automatic
       training process shall sync the data to train
       test validate folders
    """
    # root_dir needs a trailing slash (i.e. /root/dir/)
    dts = pd.date_range(sdate, edate, freq='d')

    files = []
    for i in dts:
        path_str = i.strftime("year=%Y/month=%m/day=%d/")
        print(f"{input_path}{path_str}")
        p = Path(f"{input_path}{path_str}")
        files += [f.as_posix() for f in p.rglob(f"*.{suffix}")]
    return files


def get_steps_epochs_db(meta_path, sdate, edate, batch_size, trunks, multiplier=5):
    """
    get steps epoch info for automatic run on emr
    Args:
        meta_path: path to the metadata files
        sdate: start date
        edate: end date
        batch_size: batch size for each step
        trunks: number of trunks the data is divided into
        multiplier: trunk * multiplier is the total number of epochs

    Returns: steps_per_epoch, epochs

    """
    if trunks == 1:
        return None, multiplier
    files = get_file_list_between_date(sdate=sdate, edate=edate, input_path=meta_path, suffix="csv")
    metadata = read_metadata(files)
    steps_per_epoch, epochs = get_steps_epochs(metadata=metadata, batch_size=batch_size, trunks=trunks,
                                               multiplier=multiplier)
    return steps_per_epoch, epochs


def list_tfrecord_files_db(path, train_start_date, train_days=7):
    """create a dictionary with each key represents a list of corresponding
       tfrecord files

    Args:
        path (string): path to the tfrecord files
        train_start_date: start date of training period
        train_days: number of days used for training period

    Returns:
        dict: dictionary for train validate test
    """
    train_end_date = train_start_date + timedelta(days=train_days - 1)  # end date
    validate_date = train_end_date + timedelta(days=1)
    test_date = validate_date + timedelta(days=1)
    files_dir = dict()
    files_dir[TRAIN] = get_file_list_between_date(sdate=train_start_date, edate=train_end_date, input_path=path,
                                                  suffix="gz")
    files_dir[VAL] = get_file_list_between_date(sdate=validate_date, edate=validate_date, input_path=path,
                                                suffix="gz")
    files_dir[TEST] = get_file_list_between_date(sdate=test_date, edate=test_date, input_path=path,
                                                 suffix="gz")
    return files_dir
