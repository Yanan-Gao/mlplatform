import os
import tensorflow as tf

# parse files from the input folder
def parse_input_files(path):

    files = os.listdir(path)
    files = [path + file for file in files if
                   file.startswith("part")]

    return files

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


def parse_input_single_target(model_features, dim_feature, model_targets, sw_col):

    # parser will throw error when int32 is assigned as type
    feature_description = {
        f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in
        model_features}
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
        model_features}
    # add dim feature
    feature_description.update(
        {d.name: tf.io.FixedLenFeature([], d.type, d.default_value) for d in dim_feature})

    def parse_fun(example):
        data = tf.io.parse_example(example, feature_description)
        bidid = data.pop(colname_map.BidRequestId)
        agid = data.pop(colname_map.AdGroupId)
        return data, bidid, agid
    return parse_fun


def tfrecord_parser(model_features, dim_feature, model_targets, model_type, card_cap, sw_col):

    if model_type == 'ae':
        return parse_input_ae_target(model_features, model_targets, card_cap)
    else:
        return parse_input_single_target(model_features, dim_feature, model_targets, sw_col)

def s3_copy(src_path, dest_path):
    sync_command = f"aws s3 cp {src_path} {dest_path} --recursive"
    os.system(sync_command)
    return sync_command

