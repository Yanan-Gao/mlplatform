import os

import tensorflow as tf
from tqdm import tqdm


# get all available files under s3 dir
def get_tfrecord_files(dir_list):
    """
    Get tfrecord file list from directory
    :param dir_ist: list of dir address on s3
    """
    files_result = []
    for dir in tqdm(dir_list):
        files = tf.io.gfile.listdir(dir)
        files_result.extend(
            [
                os.path.join(dir, f)
                for f in files
                if f.endswith(".tfrecord") or f.endswith(".gz")
            ]
        )

    return files_result


def tfrecord_dataset(files, batch_size, map_fn, train=True, buffer_size=10000, seed=13):
    files = tf.data.Dataset.from_tensor_slices(files)
    if train:
        return (
            files.interleave(
             lambda x: tf.data.TFRecordDataset(x, compression_type="GZIP", num_parallel_reads=tf.data.AUTOTUNE,),
             num_parallel_calls=tf.data.AUTOTUNE,
             cycle_length=tf.data.AUTOTUNE,
             deterministic=False
            )
            .shuffle(
             buffer_size=buffer_size,
             seed=seed,
             reshuffle_each_iteration=True,  # shuffle data after each epoch
            )
            .batch(
             batch_size=batch_size,
             drop_remainder=False,
             num_parallel_calls=tf.data.AUTOTUNE,
            )
            .map(
             map_fn,
             num_parallel_calls=tf.data.AUTOTUNE,  # parallel computing
             deterministic=False,  # concurrency deterministic control like synchronize setting in java
            )
            .prefetch(
             tf.data.AUTOTUNE
            )
        )

    else:
        return (
            files.interleave(
             lambda x: tf.data.TFRecordDataset(x, compression_type="GZIP", num_parallel_reads=tf.data.AUTOTUNE,),
             num_parallel_calls=tf.data.AUTOTUNE,
             cycle_length=tf.data.AUTOTUNE,
             deterministic=False
            )
            .batch(
             batch_size=batch_size,
             drop_remainder=False,
             num_parallel_calls=tf.data.AUTOTUNE,
            )
            .map(
             map_fn,
             num_parallel_calls=tf.data.AUTOTUNE,  # parallel computing
             deterministic=False,  # concurrency deterministic control like synchronize setting in java
            )
        )


# features parser for tfrecord
class feature_parser:
    def __init__(
        self, model_features, model_targets, TargetingDataIdList, Target, exp_var=True
    ):
        self.Target = Target
        self.TargetingDataIdList = TargetingDataIdList
        self.exp_var = exp_var
        self.feature_description = {}
        self.int_features = []
        for f in model_features:
            if f.type == tf.int32:
                self.feature_description.update(
                    {f.name: tf.io.FixedLenFeature([], tf.int64, f.default_value)}
                )
                self.int_features.append(f.name)
            elif f.type == tf.variant:
                # variant length generate sparse tensor
                # self.feature_description.update({f.name: tf.io.VarLenFeature(tf.int64)})
                self.feature_description.update({f.name: tf.io.FixedLenSequenceFeature((), tf.int64, default_value=0, allow_missing=True)})
            else:
                # looks like tfrecord convert the data to float32
                self.feature_description.update(
                    {f.name: tf.io.FixedLenFeature([], tf.float32, f.default_value)}
                )

        self.feature_description.update(
            {
                # to make sure that the model can handle with various length targetingdataid which will make the target
                # has various length -> we need to change the feature type of it to VarLenFeature
                # also, within a batch, TF will auto padding the VarLenFeature to the maximize length with 0
                # this change will also ask the input data type of target to become sth like [1]
                # t.name: tf.io.FixedLenFeature([], tf.float32, t.default_value)
                t.name: tf.io.FixedLenSequenceFeature([], tf.float32, default_value=0.0, allow_missing=True)
                # t.name: tf.io.VarLenFeature(tf.float32)
                for t in model_targets
            }
        )

    # return function for tf data to use for parsing
    def parser(self):

        # parse function for the tfrecorddataset
        def parse(example):
            # parse example to dict of tensor
            input_ = tf.io.parse_example(
                example, self.feature_description  # data: serial Example
            )  # schema
#             input_[self.TargetingDataIdList] = tf.sparse.to_dense(
#                 input_[self.TargetingDataIdList]
#             )
            # tf.expand_dims: add one more axis here
            # prepare for the matrix calculation in the model part
            # to work well with the embedding input of TargetingDataIdList (1,None), it is good to use exp_var
            if self.exp_var:
                input_[self.TargetingDataIdList] = tf.expand_dims(
                    input_[self.TargetingDataIdList], axis=1
                )
            target = tf.cast(input_.pop(self.Target), tf.float32)
            # target = tf.cast(tf.sparse.to_dense(input_.pop(self.Target)), tf.float32)
            for name in self.int_features:
                input_[name] = tf.cast(input_[name], tf.int32)
            return input_, target

        return parse


# # parse files from the input folder
# def parse_input_files(path):
#
#     files = os.listdir(path)
#     files = [path + file for file in files if
#                    file.startswith("part")]
#
#     return files
#
# def tfrecord_dataset(files, batch_size, map_fn):
#     return tf.data.TFRecordDataset(
#         files,
#         compression_type="GZIP",
#     ).batch(
#         batch_size=batch_size,
#         drop_remainder=False
#     ).map(
#         map_fn,
#         num_parallel_calls=tf.data.AUTOTUNE,
#         deterministic=False
#     )
#
#
# def parse_input_single_target(model_features, dim_feature, model_targets, sw_col):
#
#     # parser will throw error when int32 is assigned as type
#     feature_description = {
#         f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
#         model_features}
#     # add dim feature
#     feature_description.update(
#         {d.name: tf.io.FixedLenFeature([], d.type, d.default_value) for d in dim_feature})
#     # add target
#     feature_description.update(
#         {t.name: tf.io.FixedLenFeature([], t.type, t.default_value) for t in model_targets})
#
#     if sw_col != None:
#         feature_description.update(
#             {'Weight': tf.io.FixedLenFeature([], tf.float32, None)})
#
#     def parse_fun(example):
#
#         data = tf.io.parse_example(example, feature_description)
#         target = tf.cast(data.pop('Target'), tf.float32)
#         if sw_col == None:
#             return data, target
#         else:
#             sw = data.pop(sw_col)
#             return data, target, sw
#     return parse_fun
#
#
# def parse_input_ae_target(model_features, model_targets, card_cap):
#     # parser will throw error when int32 is assigned as type
#     feature_description = {
#         f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
#         model_features}
#
#     string_cat_targets = [f for f in model_targets if f.type == tf.string]
#     int_cat_targets = [f for f in model_targets if f.type == tf.int64]
#
#     def parse_fun(example):
#         data = tf.io.parse_example(example, feature_description)
#         cont_data = data.copy()
#         # how we construct this output will need to be aligned with the loss construction.
#         cat_data = tf.concat(
#             [tf.one_hot(tf.strings.to_hash_bucket_fast(cont_data.pop(f.name), f.cardinality), f.cardinality) for f in
#              string_cat_targets] +
#             [tf.one_hot(tf.math.floormod(cont_data.pop(f.name), f.cardinality), f.cardinality) for f in
#              int_cat_targets if f.cardinality == card_cap] +
#             [tf.one_hot(cont_data.pop(f.name), f.cardinality) for f in int_cat_targets if
#              f.cardinality != card_cap]
#             , axis=1)
#         cont_data=tf.concat([tf.expand_dims(cont_data.get(k), axis=1) for k in cont_data], axis=1)
#
#         return data, {'cont': cont_data, 'cat': cat_data}
#
#     return parse_fun
#
# def parse_scoring_data(model_features, dim_feature, colname_map):
#
#     # parser will throw error when int32 is assigned as type
#     feature_description = {
#         f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
#         model_features}
#     # add dim feature
#     feature_description.update(
#         {d.name: tf.io.FixedLenFeature([], d.type, d.default_value) for d in dim_feature})
#
#     def parse_fun(example):
#         data = tf.io.parse_example(example, feature_description)
#         bidid = data.pop(colname_map.BidRequestId)
#         agid = data.pop(colname_map.AdGroupId)
#         return data, bidid, agid
#     return parse_fun
#
#
# def tfrecord_parser(model_features, dim_feature, model_targets, model_type, card_cap, sw_col):
#
#     if model_type == 'ae':
#         return parse_input_ae_target(model_features, model_targets, card_cap)
#     else:
#         return parse_input_single_target(model_features, dim_feature, model_targets, sw_col)


def s3_copy(src_path, dest_path):
    sync_command = f"aws s3 cp {src_path} {dest_path} --recursive"
    os.system(sync_command)
    return sync_command


def s3_move(src_path, dest_path):
    sync_command = f"aws s3 mv --recursive {src_path} {dest_path}"
    os.system(sync_command)
    return sync_command
