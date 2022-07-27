import os
from typing import List

import tensorflow as tf
import tensorflow_probability as tfp
from pathlib import Path
import math
import pandas as pd
import numpy as np
import itertools
from tensorflow_serving.apis import predict_pb2
from datetime import datetime, timedelta

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


def tfrecord_files_dict(path):
    p = Path(f"{path}")
    files = [str(f.resolve()) for f in list(p.glob("*.gz"))]
    if len(files) == 0:
        files_dict = {x.name: [str(f.resolve()) for f in list(Path(f"{path}/{x.name}/").glob("*.gz"))] for x in
                      p.iterdir()
                      if x.is_dir()}
    else:
        files_dict = {"train": files}
    return files_dict


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


def lookback_csv_dataset_generator(end_date_string,
                                   path_prefix,
                                   batch_size,
                                   selected_cols,
                                   label_name="mb2w",
                                   glob_pattern='**/*.csv',
                                   lookback=7,
                                   holdout_days=1,
                                   hvd_rank=None,
                                   hvd_size=None,
                                   shuffle=False,
                                   seed=42,
                                   ):
    import tensorflow as tf

    def _generator(csv_file, batch_size, drop_remainder=True):
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
        return tf.data.Dataset.from_generator(
            _generator,
            output_signature=(
                {k: tf.TensorSpec(shape=(None,), dtype=v) for k, v in selected_cols.items() if k != label_name},
                tf.TensorSpec(shape=(None,), dtype=tf.float32)
            ),
            args=(file, batch_size,)
        )

    def _interleaved_generator_dataset(files):
        return tf.data.Dataset.from_tensor_slices(files).interleave(
            lambda x: _generator_dataset(x, batch_size, selected_cols),
            num_parallel_calls=tf.data.AUTOTUNE,
            deterministic=False
        )

    def _date_range(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def _get_files(paths, train_start_date, train_end_date, holdout_date, shuffle=False, split=None, rank=None):
        print(f"{train_start_date} -- {train_end_date}")
        tr, val, ts = [], [], []

        paths = [paths] if not isinstance(paths, list) else paths

        for path in paths:
            tr.append(
                [file.as_posix() for d in _date_range(train_start_date, train_end_date) for file in
                 Path(f"{path}year={d.year}/month={d.month:02}/day={d.day:02}/").glob(glob_pattern)]
            )

            holdout_files = [i.as_posix() for i in Path(
                f"{path}year={holdout_date.year}/month={holdout_date.month:02}/day={holdout_date.day:02}/").glob(
                glob_pattern)]
            _val_split = len(holdout_files) // 2
            val.append(holdout_files[:_val_split])
            ts.append(holdout_files[_val_split:])

        def _round_robin_shuffle_split(files_list, shuffle=False, split=None, rank=None):
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

    holdout_date = datetime.strptime(end_date_string, "%Y-%m-%d")
    train_start_date = holdout_date - timedelta(days=lookback)
    train_end_date = holdout_date - timedelta(days=holdout_days - 1)

    tr_files, val_files, ts_files = _get_files(path_prefix, train_start_date, train_end_date, holdout_date, shuffle,
                                               hvd_size, hvd_rank)

    return _interleaved_generator_dataset(tr_files), \
           _interleaved_generator_dataset(val_files), \
           _interleaved_generator_dataset(ts_files)


def downsample(files_dict, down_sample_rate=None):
    if down_sample_rate is not None:
        files_dict_sample = {}
        for k, v in files_dict.items():
            files_dict_sample[k] = files_dict[k][: math.ceil(len(v) * down_sample_rate)]
        return files_dict_sample

    return files_dict


def create_datasets_dict(files_dict, batch_size, model_features, model_targets, eval_batch_size=None):
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


def generate_random_tensors(model_features, model_targets, num_examples=1000):
    features = {}
    for f in model_features:
        if f.type == tf.int32:
            features[f.name] = tf.constant(np.random.randint(0, f.cardinality, size=num_examples), dtype=tf.int32)

        elif f.type == tf.float32:
            features[f.name] = tf.constant(np.random.random_sample(size=num_examples).astype(np.float32),
                                           dtype=tf.float32)

    targets = []
    for t in model_targets:
        if t.binary:
            targets.append(
                tf.constant(np.random.binomial(n=1, p=0.3, size=num_examples).astype(np.float32), dtype=tf.float32))
        else:
            targets.append(tf.constant(np.random.random_sample(size=num_examples).astype(np.float32), dtype=tf.float32))

    return features, tf.stack(targets, axis=-1)


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
            grpc_request.inputs[f.name].CopyFrom(
                tf.make_tensor_proto(np.random.randint(0, f.cardinality, size=1).astype(np.int32)))

        elif f.type == tf.float32:
            grpc_request.inputs[f.name].CopyFrom(tf.make_tensor_proto([[0.0]]))

    return grpc_request
