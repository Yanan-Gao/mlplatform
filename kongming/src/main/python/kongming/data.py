import os
import tensorflow as tf
import pandas as pd
from tensorflow.python.data.ops.dataset_ops import DatasetV2

def csv_dataset(files, batch_size, selected_features, label_name, weight_name):
    def _generator(csv_file, drop_remainder=False):
        usecols_list = [[v.column_name] if v.column_name == v.name else v.column_name for k, v in selected_features.items()]
        usecols = [col for cols in usecols_list for col in cols]
        with pd.read_csv(csv_file.decode(),
                         chunksize=batch_size,
                         usecols = usecols,
                         ) as csv_chunks:
            for csv_chunk in csv_chunks:

                # skip is not full batch
                if drop_remainder & (csv_chunk.shape[0] != batch_size): continue

                # TODO: should really remove NA upstream
                for k, v in selected_features.items():
                    if not v.default_value is None and not isinstance(v.default_value, list):
                        csv_chunk[v.name].fillna(v.default_value, inplace=True)
                label = csv_chunk.pop(label_name)
                weight = csv_chunk.pop(weight_name)

                # for simple features
                cols = [k for k, v in selected_features.items() if
                        k not in [label_name, weight_name] and v.column_name == v.name]
                features = dict(csv_chunk[cols])

                # for list features
                for k, v in selected_features.items():
                    if v.column_name != v.name:
                        features[v.name] = csv_chunk[v.column_name]

                yield (features, label.values, weight.values)

    def _generator_dataset(file):
        spec = {k: tf.TensorSpec(shape=(None,) if not isinstance(feature.default_value, list) else (None, len(feature.default_value)), dtype=feature.base_type)
            for k, feature in selected_features.items() if k not in [label_name, weight_name] }
        dataset = tf.data.Dataset.from_generator(
            _generator,
            output_signature=(
                spec,
                tf.TensorSpec(shape=(None,), dtype=tf.float32),
                tf.TensorSpec(shape=(None,), dtype=tf.float32)
            ),
            args=(file,)
        )

        return dataset

    def interleaved_generator_dataset(files):
        options = tf.data.Options()
        options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF

        return tf.data.Dataset.from_tensor_slices(files).with_options(options).interleave(
            lambda x: _generator_dataset(x),
            num_parallel_calls=tf.data.AUTOTUNE,
            deterministic=False
        )

    return interleaved_generator_dataset(files)

def tfrecord_dataset(files, batch_size, map_fn):
    return tf.data.TFRecordDataset(
        files,
        compression_type="GZIP",
    ).batch(
        batch_size=batch_size,
        drop_remainder=False
    ).map(
        map_fn,
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False
    )

def cache_prefetch_dataset(dataset: DatasetV2):
    return dataset.cache().prefetch(tf.data.AUTOTUNE)

def parse_input_single_target(model_features, dim_feature, model_targets, sw_col):
    # parser will throw error when int32 is assigned as type
    feature_description = {
        f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
        model_features if f.type != tf.variant
    }

    # will need to change for fixed-len float list
    feature_description.update({
        f.name: tf.io.VarLenFeature(dtype=tf.int64) for f in
        model_features if f.type == tf.variant
    })

    # add dim feature
    feature_description.update(
        {d.name: tf.io.FixedLenFeature([], d.type, d.default_value) for d in dim_feature})
    # add target
    feature_description.update(
        {t.name: tf.io.FixedLenFeature([], t.type, t.default_value) for t in model_targets})

    if sw_col != None:
        feature_description.update(
            {'Weight': tf.io.FixedLenFeature([], tf.float32, None)})

    def parse_fun(example):

        data = tf.io.parse_example(example, feature_description)
        for f in model_features:
            if f.type == tf.variant:
                data[f.name] = tf.sparse.to_dense(data[f.name])
                l = tf.shape(data[f.name])[1]
                data[f.name] = tf.pad(data[f.name], [[0, 0], [0, len(f.default_value) - l]])

        target = tf.cast(data.pop('Target'), tf.float32)
        if sw_col == None:
            return data, target
        else:
            sw = data.pop(sw_col)
            return data, target, sw
    return parse_fun


def parse_input_ae_target(model_features, model_targets, card_cap):
    # parser will throw error when int32 is assigned as type
    feature_description = {
        f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
        model_features}

    string_cat_targets = [f for f in model_targets if f.type == tf.string]
    int_cat_targets = [f for f in model_targets if f.type == tf.int64]

    def parse_fun(example):
        data = tf.io.parse_example(example, feature_description)
        cont_data = data.copy()
        # how we construct this output will need to be aligned with the loss construction.
        cat_data = tf.concat(
            [tf.one_hot(tf.strings.to_hash_bucket_fast(cont_data.pop(f.name), f.cardinality), f.cardinality) for f in
             string_cat_targets] +
            [tf.one_hot(tf.math.floormod(cont_data.pop(f.name), f.cardinality), f.cardinality) for f in
             int_cat_targets if f.cardinality == card_cap] +
            [tf.one_hot(cont_data.pop(f.name), f.cardinality) for f in int_cat_targets if
             f.cardinality != card_cap]
            , axis=1)
        cont_data=tf.concat([tf.expand_dims(cont_data.get(k), axis=1) for k in cont_data], axis=1)

        return data, {'cont': cont_data, 'cat': cat_data}

    return parse_fun

def parse_scoring_data(model_features, dim_feature, colname_map):
    # parser will throw error when int32 is assigned as type
    feature_description = {
        f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
        model_features if f.type != tf.variant
    }

    # will need to change for fixed-len float list
    feature_description.update({
        f.name: tf.io.VarLenFeature(dtype=tf.int64) for f in
        model_features if f.type == tf.variant
    })

    # add dim feature
    feature_description.update(
        {d.name: tf.io.FixedLenFeature([], d.type, d.default_value) for d in dim_feature})

    def parse_fun(example):
        data = tf.io.parse_example(example, feature_description)
        for f in model_features:
            if f.type == tf.variant:
                data[f.name] = tf.sparse.to_dense(data[f.name])
                l = tf.shape(data[f.name])[1]
                data[f.name] = tf.pad(data[f.name], [[0, 0], [0, len(f.default_value) - l]])
        bidid = data.pop(colname_map.BidRequestId)
        agid = data.pop(colname_map.AdGroupId)
        return data, bidid, agid
    return parse_fun


def tfrecord_parser(model_features, dim_feature, model_targets, model_type, card_cap, sw_col):

    if model_type == 'ae':
        return parse_input_ae_target(model_features, model_targets, card_cap)
    else:
        return parse_input_single_target(model_features, dim_feature, model_targets, sw_col)

