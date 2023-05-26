import os
from typing import List
from datetime import timedelta, datetime
import random
import warnings
import tensorflow as tf
from pathlib import Path
import math
import pandas as pd
import numpy as np
import itertools
from tensorflow_serving.apis import predict_pb2

from philo.features import get_map_function, get_map_function_test, get_map_function_weighted

TRAIN = "train"
VAL = "validation"
TEST = "test"


def s3_sync(src_path, dst_path, quiet=True):
    """
    Sync data from source path to destination path, need awscli
    Args:
        src_path: source path
        dst_path: destination path

    Returns: command for syncing

    """
    sync_command = f"aws s3 sync {src_path} {dst_path}"
    if quiet:
        sync_command = sync_command + " --quiet"
    os.system(sync_command)
    return sync_command


def s3_copy(src_path, dest_path, quiet=True):
    """
    Copy data from source path to destination path, need awscli
    Args:
        src_path: source path
        dest_path: destination path
        quiet: print out copy message or not

    Returns:command for copying

    """
    cp_command = f"aws s3 cp {src_path} {dest_path} --recursive"
    if quiet:
        cp_command = cp_command + " --quiet"
    os.system(cp_command)
    return cp_command

def s3_write_success_file(dest_path, quiet=True):
    """
    writes _SUCCESS file to destination path, need awscli
    Args:
        dest_path: destination path
        quiet: print out copy message or not

    Returns:command for copying

    """
    cp_command = f"echo -n | aws s3 cp - {dest_path}/_SUCCESS"
    if quiet:
        cp_command = cp_command + " --quiet"
    os.system(cp_command)
    return cp_command


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
    # if trunks == 1:
    #     return None, multiplier
    files = get_meta_files_emr(path)
    metadata = read_metadata(files)
    steps_per_epoch, epochs = get_steps_epochs(metadata=metadata, batch_size=batch_size,
                                               trunks=trunks, multiplier=multiplier)
    return steps_per_epoch, epochs


def list_tfrecord_files(path, data_format="GZIP"):
    """
    create a dictionary with each key represents a list of corresponding
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


def tfrecord_dataset(files, batch_size, map_fn, compression_type="GZIP", prefetch_num=10, repeat=False, drop_remainder = False):
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
        drop_remainder: if batch_size does not evenly divide dataset, whether to drop the remainder or not

    Returns: tf data pipeline

    """
    data = tf.data.TFRecordDataset(files, compression_type=compression_type)
    if repeat:
        data = data.repeat()

    return data.batch(
        batch_size=batch_size,
        drop_remainder=drop_remainder
    ).map(
        map_func=map_fn,
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False
    ).prefetch(
        prefetch_num
    )

def lookback_csv_dataset_generator(
                                   path_prefix,
                                   batch_size,
                                   selected_cols,
                                   label_name="label",
                                   glob_pattern='/*.csv',
                                   hvd_rank=None,
                                   hvd_size=None,
                                   shuffle=False,
                                   seed=42,
                                   ):
    """
    create csv data pipeline
    Args:
        path_prefix: path prefix to load data from -- expects files to be in form path_prefix/train/glob_pattern
        batch_size: batch size for train
        selected_cols: list of features
        label_name: name of model target
        glob_pattern: file format glob pattern
        hvd_rank: splits dataset with hvd_rank
        hvd_size: splits dataset into hvd_size
        shuffle: randomly shuffle the data
        seed: seed to use for random shuffle
    Returns: csv dataloader

    """

    def _generator(csv_file, batch_size, drop_remainder=False):
        """
        create generator for csv dataset
        Args:
            csv_file: path to csv file
            batch_size: batch size for train
            drop_remainder: if batch_size does not evenly divide dataset, whether to drop the remainder or not

        Returns: generator function for reading csv

        """
        with pd.read_csv(csv_file.decode(),
                         chunksize=batch_size,
                         usecols=[k for k, v in selected_cols.items()],
                         ) as csv_chunks:
            for csv_chunk in csv_chunks:

                # skip is not full batch
                if drop_remainder & (csv_chunk.shape[0] != batch_size): continue

                # TODO: should really remove NA upstream
                csv_chunk.fillna(0, inplace=True)
                label = csv_chunk.pop(label_name)
                yield (dict(csv_chunk), label.values)


    def _generator_dataset(file, batch_size, selected_cols):
        """
        create dataset from generator
        Args:
            file: path to csv file
            batch_size: batch size for train
            selected_cols: list of columns to use for dataset

        Returns: tensorflow dataset

        """
        return tf.data.Dataset.from_generator(
            _generator,
            output_signature=(
                {k: tf.TensorSpec(shape=(None,), dtype=v) for k, v in selected_cols.items() if k != label_name},
                tf.TensorSpec(shape=(None,), dtype=tf.float32)
            ),
            args=(file, batch_size,)
        )

    def _interleaved_generator_dataset(files):
        """
        interleaves multiple tf generator datasets for parallel data loading
        Args:
            files: list of paths to csv files
        Returns: tensorflow dataset

        """
        options = tf.data.Options()
        options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF

        return tf.data.Dataset.from_tensor_slices(files) \
            .with_options(options) \
            .interleave(
            #map_function
            lambda x: _generator_dataset(x, batch_size, selected_cols),
            num_parallel_calls=tf.data.AUTOTUNE,
            deterministic=False
        )

    def _date_range(start_date, end_date):
        """
        creates range of dates
        Args:
            start_date: first date in range
            end_date: last date in range
        Returns: date range

        """
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def _get_files(paths,  shuffle=False, split=None, rank=None):
        """
        gets list of files from list of path prefixes
        Args:
            paths: list of path prefixes
            shuffle: shuffles the paths
            split: size of data split
            rank: rank of data split
        Returns: list of file paths for training, validation, and test

        """
        tr, val, ts = [], [], []

        paths = [paths] if not isinstance(paths, list) else paths

        for path in paths:
            tr.append(
                [file.as_posix() for file in
                 Path(f"{path}train/").glob(glob_pattern)]
            )
            val.append(
                       [file.as_posix() for file in
                        Path(f"{path}validation/").glob(glob_pattern)]
                   )
            ts.append(
                            [file.as_posix() for file in
                             Path(f"{path}test/").glob(glob_pattern)]
                        )

        def _round_robin_shuffle_split(files_list, shuffle=False, split=None, rank=None):
            """
            shuffles and splits files
            Args:
                files_list: list of files
                shuffle: whether or not to shuffle the files
                split: size of data split
                rank: rank of data split
            Returns: list of file paths for training, validation, and test

            """
            files = list(itertools.chain(*zip(*files_list)))
            if shuffle:
                np.random.shuffle(files)

            if split:
                return np.array_split(files, split)[rank]
            else:
                return files

        tr_files = _round_robin_shuffle_split(files_list=tr, shuffle=shuffle, split=split, rank=rank)
        val_files = _round_robin_shuffle_split(files_list=val, shuffle=shuffle, split=split, rank=rank)
        ts_files = _round_robin_shuffle_split(files_list=ts, shuffle=shuffle, split=split, rank=rank)
        return tr_files, val_files, ts_files



    tr_files, val_files, ts_files = _get_files(path_prefix, shuffle,
                                               hvd_size, hvd_rank)

    return _interleaved_generator_dataset(tr_files), \
           _interleaved_generator_dataset(val_files), \
           _interleaved_generator_dataset(ts_files)



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
    # create train, validate and test datasets
    for split, files in files_dict.items():
        if split == TRAIN:
            # create train dataset
            ds[split] = tfrecord_dataset(files=files,
                                         batch_size=batch_size,
                                         map_fn=map_function,
                                         compression_type=compression_type,
                                         prefetch_num=prefetch_num, repeat=repeat
                                         )
        elif split == TEST:
            if offline_test:
                # if offline test, we need to generate bidRequestId for further analysis
                map_test_func = get_map_function_test(model_features, model_target, "BidRequestId")
                ds[split] = tfrecord_dataset(files=files,
                                             batch_size=test_batch_size if test_batch_size is not None else batch_size,
                                             map_fn=map_test_func,
                                             compression_type=compression_type
                                             )
            else:
                # if online test, we do not need to generate bidRequestId
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
    Args:
        model_features: list of model features
        model_target: model target
        batch_size: batch size
    """

    def generate_tensor_slices(num):
        x, y = generate_random_pandas(model_features, model_target, num)
        return dict(x), y

    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF

    return {
        TRAIN: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(1000)).withOptions(options).batch(batch_size),
        VAL: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).withOptions(options).batch(batch_size),
        TEST: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).withOptions(options).batch(batch_size)
    }


def prepare_real_data(model_features, model_target, input_path,
                      batch_size, eval_batch_size, prefetch_num=10,
                      offline_test=False, repeat=False, weight_name=None,
                      weight_value=None, data_format = "csv"):
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
        data_format: csv or tfrecord fileformat

    Returns: dictionary of dataset

    """
    # print(input_path)
    if data_format != "csv":
        files = list_tfrecord_files(input_path)
        # validate files and generate files dictionary if VAL or TEST is not in files
        files = validate_and_generate_files_dict(files)
    if weight_name is not None and weight_value is not None:
        map_func = get_map_function_weighted(model_features, model_target, weight_name, weight_value)
    elif weight_name is None and weight_value is None:
        map_func = get_map_function(model_features, model_target)
    else:
        raise Exception('weight_name and weight have to be both not None to use the sample_weight')

    if data_format == "csv":
        selected_cols = {f.name: f.type for f in model_features}
        selected_cols["label"] = tf.int64
        ds_tr, ds_val, ds_ts = lookback_csv_dataset_generator(
              path_prefix=[input_path],
              batch_size= batch_size,
              selected_cols=selected_cols,
              glob_pattern = "*.csv"
          )
        ds = {TRAIN: ds_tr, VAL: ds_val, TEST: ds_ts}
    else:
        ds = create_datasets(files_dict=files, batch_size=batch_size, model_features=model_features,
                                        model_target=model_target, map_function=map_func, compression_type="GZIP",
                                        eval_batch_size=eval_batch_size, prefetch_num=prefetch_num, offline_test=offline_test,
                                        repeat=repeat)

    return ds


def validate_and_generate_files_dict(files_dict):
    """
    validate the files dict
    if length is 0, raise exception, if length is 1, print only one file and output the key of the item
    if length is 2, if TRAIN not in the key, raise exception, if TRAIN in the key,
    find out the key of the other item, shuffle the file list and split the data into TEST and VAL, then return the new dict
    if length is 3, return the original dict
    Args:
        files_dict: dictionary of files
    returns:
        new dictionary of files if length is 2, otherwise return the original dictionary if length is 3, otherwise
        raise exception if length is 0 or 1
    """
    empty_removed = dict(filter(lambda pair: True if pair[1] else False, files_dict.items()))
    if len(empty_removed) == 0:
        raise Exception('No files found in the input path')
    elif len(empty_removed) == 1:
        raise Exception(f'Only {list(files_dict.keys())[0]} found in the input path')
    elif len(empty_removed) == 2:
        if TRAIN not in empty_removed.keys():
            raise Exception('No training data found in the input path')
        else:
            key = list(set(empty_removed.keys()) - {TRAIN})[0]
            # copy files_dict[key] to test_val to avoid changing the original list
            # create a warning message stating that only key is found, will split it to VAL and TEST evenly
            warnings.warn(f'Only {key} found in the input path, will split it to VAL and TEST evenly')
            test_val = empty_removed[key][:]
            random.shuffle(test_val)
            new_file_dict = {TRAIN: empty_removed[TRAIN], VAL: test_val[:len(test_val) // 2],
                             TEST: test_val[len(test_val) // 2:]}
            return new_file_dict
    elif len(empty_removed) == 3:
        return files_dict
    else:
        raise Exception('More than 3 files found in the input path')


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
