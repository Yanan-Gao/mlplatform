# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Functions for loading and parsing tfrecords files from S3
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
import tensorflow as tf
from tqdm import tqdm
import os

# get all available files under s3 dir
def get_tfrecord_files(dir_list):
    """
    Get tfrecord file list from directory
    :param dir_ist: list of dir address on s3
    """
    files_result = []
    for dir in tqdm(dir_list):
        files = tf.io.gfile.listdir(dir)
        files_result.extend([
            os.path.join(dir, f)
            for f in files
            if f.endswith(".tfrecord") or f.endswith(".gz")
        ])

    return files_result


def emb_sz_rule(n_cat, DEFAULT_EMB_DIM = 16):
    "Rule of thumb to pick embedding size corresponding to `n_cat`"
    return min(DEFAULT_EMB_DIM, round(1.6 * n_cat ** 0.56))


# features parser for tfrecord files
class feature_parser:
    def __init__(self, model_features, model_targets, Target, target_length, exp_var=True):
        self.Target = Target
        self.exp_var = exp_var
        self.feature_description = {}
        for f in model_features:
            if f.type == tf.int32:
                self.feature_description.update({f.name: tf.io.FixedLenFeature([], tf.int64, f.default_value)})
            elif f.type == tf.variant:
                # variant length generate sparse tensor
                self.feature_description.update({f.name: tf.io.VarLenFeature(tf.int64)})
            else:
                # looks like tfrecord convert the data to float32
                self.feature_description.update({f.name: tf.io.FixedLenFeature([], tf.float32, f.default_value)})

        self.feature_description.update(
            {t.name: tf.io.FixedLenFeature([target_length], tf.int64, t.default_value) for t in model_targets})

    # return function for tf data to use for parsing
    def parser(self):

        # parse function for the tfrecorddataset
        def parse(example):
            # parse example to dict of tensor
            input_ = tf.io.parse_example(example,  # data: serial Example
                                         self.feature_description)  # schema
            # tf.expand_dims: add one more axis here
            # prepare for the matrix calculation in the model part
            input_[self.Target] = tf.cast(input_[self.Target], tf.float32)
            target = tf.cast(input_.pop(self.Target), tf.float32)

            return input_, target

        return parse


def tfrecord_dataset(files, batch_size, map_fn, train=True, buffer_size=10000):
    if train:
        return tf.data.TFRecordDataset(
            files,
            compression_type="GZIP",
        ).shuffle(
            buffer_size=buffer_size,
            seed=13,
            reshuffle_each_iteration=True,  # shuffle data after each epoch
        ).batch(
            batch_size=batch_size,
            drop_remainder=True  # make sure each batch has the same batch size
        ).map(
            map_fn,
            num_parallel_calls=tf.data.AUTOTUNE,  # parallel computing
            deterministic=False  # concurrency deterministic control like synchronize setting in java
        )

    else:
        return tf.data.TFRecordDataset(
            files,
            compression_type="GZIP",
        ).batch(
            batch_size=batch_size,
            drop_remainder=False  # make sure each batch has the same batch size
        ).map(
            map_fn,
            num_parallel_calls=tf.data.AUTOTUNE,  # parallel computing
            deterministic=False  # concurrency deterministic control like synchronize setting in java
        )