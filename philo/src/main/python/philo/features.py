from collections import namedtuple, OrderedDict
from itertools import chain
from copy import copy

import tensorflow as tf
from tensorflow.keras.initializers import RandomNormal, Zeros
from tensorflow.keras.layers import Input, Lambda

# ctr imports
from philo.layers import concat_func, Linear, combined_dnn_input
from philo.inputs import create_embedding_matrix, embedding_lookup, get_dense_input, varlen_embedding_lookup, \
    get_varlen_pooling_list, mergeDict
from philo.feature_utils import Target

SEED = 1024
DEFAULT_MODEL_TARGET = Target(name='label', type=tf.int64, default_value=0, enabled=True, binary=True)

DEFAULT_GROUP_NAME = "default_group"

ADGROUP_FEATURE_LIST = ['AdGroupId', 'AdvertiserId', 'CreativeId']


class VarLenSparseFeat(namedtuple('VarLenSparseFeat',
                                  ['sparsefeat', 'maxlen', 'combiner',
                                   'length_name', 'weight_name', 'weight_norm'])):
    """
    Currently not used, might be useful if contextual information is introduced
    """
    __slots__ = ()

    def __new__(cls, sparsefeat, maxlen, combiner="mean", length_name=None, weight_name=None, weight_norm=True):
        return super(VarLenSparseFeat, cls).__new__(cls, sparsefeat, maxlen, combiner, length_name, weight_name,
                                                    weight_norm)

    @property
    def name(self):
        return self.sparsefeat.name

    @property
    def vocabulary_size(self):
        return self.sparsefeat.vocabulary_size

    @property
    def embedding_dim(self):
        return self.sparsefeat.embedding_dim

    @property
    def use_hash(self):
        return self.sparsefeat.use_hash

    @property
    def vocabulary_path(self):
        return self.sparsefeat.vocabulary_path

    @property
    def dtype(self):
        return self.sparsefeat.dtype

    @property
    def embeddings_initializer(self):
        return self.sparsefeat.embeddings_initializer

    @property
    def embedding_name(self):
        return self.sparsefeat.embedding_name

    @property
    def group_name(self):
        return self.sparsefeat.group_name

    @property
    def trainable(self):
        return self.sparsefeat.trainable

    def __hash__(self):
        return self.name.__hash__()


class SparseFeat(namedtuple('SparseFeat',
                            ['name', 'vocabulary_size', 'embedding_dim', 'use_hash', 'vocabulary_path', 'dtype',
                             'embeddings_initializer',
                             'embedding_name',
                             'group_name', 'trainable'])):
    __slots__ = ()

    def __new__(cls, name, vocabulary_size, embedding_dim=4, use_hash=False, vocabulary_path=None, dtype="int32",
                embeddings_initializer=None,
                embedding_name=None,
                group_name=DEFAULT_GROUP_NAME, trainable=True):

        if embedding_dim == "auto":
            embedding_dim = 6 * int(pow(vocabulary_size, 0.25))
        if embeddings_initializer is None:
            embeddings_initializer = RandomNormal(mean=0.0, stddev=0.0001, seed=SEED)

        if embedding_name is None:
            embedding_name = name

        return super(SparseFeat, cls).__new__(cls, name, vocabulary_size, embedding_dim, use_hash, vocabulary_path,
                                              dtype,
                                              embeddings_initializer,
                                              embedding_name, group_name, trainable)

    def __hash__(self):
        return self.name.__hash__()


class DenseFeat(namedtuple('DenseFeat', ['name', 'dimension', 'dtype', 'transform_fn'])):
    """ Dense feature
    Args:
        name: feature name,
        dimension: dimension of the feature, default = 1.
        dtype: dtype of the feature, default="float32".
        transform_fn: If not `None` , a function that can be used to transform
        values of the feature.  the function takes the input Tensor as its
        argument, and returns the output Tensor.
        (e.g. lambda x: (x - 3.0) / 4.2).
    """
    __slots__ = ()

    def __new__(cls, name, dimension=1, dtype="float32", transform_fn=None):
        return super(DenseFeat, cls).__new__(cls, name, dimension, dtype, transform_fn)

    def __hash__(self):
        return self.name.__hash__()


# def get_feature_names(feature_columns):
#     features = build_input_features(feature_columns)
#     return list(features.keys())


def build_input_features(feature_columns, prefix=''):
    """
    create input layer
    Args:
        feature_columns: list of SparseFeat or DenseFeat
        prefix: name prefix for the tensor

    Returns:

    """
    input_features = OrderedDict()
    for fc in feature_columns:
        if isinstance(fc, SparseFeat):
            input_features[fc.name] = Input(
                shape=(1,), name=prefix + fc.name, dtype=fc.dtype)
        elif isinstance(fc, DenseFeat):
            input_features[fc.name] = Input(
                shape=(fc.dimension,), name=prefix + fc.name, dtype=fc.dtype)
        elif isinstance(fc, VarLenSparseFeat):
            input_features[fc.name] = Input(shape=(fc.maxlen,), name=prefix + fc.name,
                                            dtype=fc.dtype)
            if fc.weight_name is not None:
                input_features[fc.weight_name] = Input(shape=(fc.maxlen, 1),
                                                       name=prefix + fc.weight_name,
                                                       dtype="float32")
            if fc.length_name is not None:
                input_features[fc.length_name] = Input((1,), name=prefix + fc.length_name,
                                                       dtype='int32')

        else:
            raise TypeError("Invalid feature column type,got", type(fc))

    return input_features


def get_linear_logit(features, feature_columns, units=1, use_bias=False, seed=SEED, prefix='linear',
                     l2_reg=0, sparse_feat_refine_weight=None):
    """
    get wide part of the dual tower structure
    Args:
        features: input feature tensors
        feature_columns: An iterable containing list of linear feature columns
        units: always equal to 1, not changed, didn't remove it to avoid unnecessary bug
        use_bias: use bias in linear logit or not
        seed: random seed
        prefix: name prefix for linear logit
        l2_reg: l2 regularization weights for linear logit
        sparse_feat_refine_weight: sparse feature weights, not used in deepfm

    Returns:

    """
    linear_feature_columns = copy(feature_columns)
    # get the original raw feature out as one of the 1st order input of FM
    for i in range(len(linear_feature_columns)):
        if isinstance(linear_feature_columns[i], SparseFeat):
            linear_feature_columns[i] = linear_feature_columns[i]._replace(
                embedding_dim=1, embeddings_initializer=Zeros())
        if isinstance(linear_feature_columns[i], VarLenSparseFeat):
            linear_feature_columns[i] = linear_feature_columns[i]._replace(
                sparsefeat=linear_feature_columns[i].sparsefeat._replace(
                    embedding_dim=1, embeddings_initializer=Zeros()))
    # get the embedding linear logit, which represents the one hot encoding
    linear_emb_list = [input_from_feature_columns(features, linear_feature_columns, l2_reg, seed,
                                                  prefix=prefix + str(i))[0] for i in range(units)]
    # get the dense input for fm
    _, dense_input_list = input_from_feature_columns(features, linear_feature_columns, l2_reg, seed, prefix=prefix)

    linear_logit_list = []
    for i in range(units):

        if len(linear_emb_list[i]) > 0 and len(dense_input_list) > 0:
            sparse_input = concat_func(linear_emb_list[i])
            dense_input = concat_func(dense_input_list)
            if sparse_feat_refine_weight is not None:
                sparse_input = Lambda(lambda x: x[0] * tf.expand_dims(x[1], axis=1))(
                    [sparse_input, sparse_feat_refine_weight])
            linear_logit = Linear(l2_reg, mode=2, use_bias=use_bias, seed=seed)([sparse_input, dense_input])
        elif len(linear_emb_list[i]) > 0:
            sparse_input = concat_func(linear_emb_list[i])
            if sparse_feat_refine_weight is not None:
                sparse_input = Lambda(lambda x: x[0] * tf.expand_dims(x[1], axis=1))(
                    [sparse_input, sparse_feat_refine_weight])
            linear_logit = Linear(l2_reg, mode=0, use_bias=use_bias, seed=seed)(sparse_input)
            # add on top of existing code to align dimensions, revert following line if code breaks somewhere downstream
            linear_logit = tf.squeeze(linear_logit, axis=-1)
        elif len(dense_input_list) > 0:
            dense_input = concat_func(dense_input_list)
            linear_logit = Linear(l2_reg, mode=1, use_bias=use_bias, seed=seed)(dense_input)
        else:  # empty feature_columns
            return Lambda(lambda x: tf.constant([[0.0]]))(list(features.values())[0])
        linear_logit_list.append(linear_logit)

    return concat_func(linear_logit_list)


def input_from_feature_columns(features, feature_columns, l2_reg, seed, prefix='', seq_mask_zero=True,
                               support_dense=True, support_group=False):
    """
    Create inputs from the model features by separating dense features and embedded features
    Args:
        features: list of input feature tensors
        feature_columns: list of SparseFeat, VarlenFeat and DenseFeat
        l2_reg: l2 regularization weights
        seed: random seed
        prefix: prefix for the name of the tensor
        seq_mask_zero: whether mask sequence
        support_dense: whether support dense features
        support_group: whether support grouping features, not used in deepfm, but if used, it means that there are
                       certain groups of features that could only interact with the features within the group

    Returns: embedding list and dense list

    """
    sparse_feature_columns = list(
        filter(lambda x: isinstance(x, SparseFeat), feature_columns)) if feature_columns else []
    varlen_sparse_feature_columns = list(
        filter(lambda x: isinstance(x, VarLenSparseFeat), feature_columns)) if feature_columns else []

    embedding_matrix_dict = create_embedding_matrix(feature_columns, l2_reg, seed, prefix=prefix,
                                                    seq_mask_zero=seq_mask_zero)
    group_sparse_embedding_dict = embedding_lookup(embedding_matrix_dict, features, sparse_feature_columns)
    dense_value_list = get_dense_input(features, feature_columns)
    if not support_dense and len(dense_value_list) > 0:
        raise ValueError("DenseFeat is not supported in dnn_feature_columns")

    sequence_embed_dict = varlen_embedding_lookup(embedding_matrix_dict, features, varlen_sparse_feature_columns)
    group_varlen_sparse_embedding_dict = get_varlen_pooling_list(sequence_embed_dict, features,
                                                                 varlen_sparse_feature_columns)
    group_embedding_dict = mergeDict(group_sparse_embedding_dict, group_varlen_sparse_embedding_dict)
    if not support_group:
        group_embedding_dict = list(chain.from_iterable(group_embedding_dict.values()))
    return group_embedding_dict, dense_value_list


def get_map_function(model_features, model_target):
    """create function for transforming data during tf data pipeline

    Args:
        model_features (list): feature definition
        model_target (nametuple): target nametuple

    Returns:
        function: function for transforming
    """

    feature_description = {f.name: (tf.io.FixedLenFeature(1, f.type, f.default_value) if f.max_len == 0 else tf.io.VarLenFeature(dtype=f.type)) for f
                                                         in model_features }

    feature_description[model_target.name] = tf.io.FixedLenFeature(1, model_target.type, model_target.default_value)

    def _parse_examples(serial_exmp):

        features = tf.io.parse_example(serial_exmp, features=feature_description)
        for f in model_features:
            key = f.name
            if f.max_len > 0:
                features[key] = tf.sparse.to_dense(features[key])
        label = features.pop(model_target.name)
        return features, label

    return _parse_examples


def get_map_function_test(model_features, model_target, keep_id):
    """
    get map function for testing data with a keep_id (BidRequestId)
    Args:
        model_features: model feature list
        model_target: model target
        keep_id: kept id name

    Returns:
        mapping function
    """
    feature_description = {f.name: (tf.io.FixedLenFeature(1, f.type, f.default_value) if f.max_len==0 else tf.io.VarLenFeature(dtype=f.type)) for f
                                                         in model_features }

    feature_description[model_target.name] = tf.io.FixedLenFeature(1, model_target.type, 0)

    feature_description[keep_id] = tf.io.FixedLenFeature(1, tf.string, '0')

    def _parse_examples(serial_exmp):
        features = tf.io.parse_example(serial_exmp, features=feature_description)
        for f in model_features:
                    key = f.name
                    if f.max_len > 0:
                        features[key] = tf.sparse.to_dense(features[key])
        labels = features.pop(model_target.name)
        ids = features.pop(keep_id)
        return features, labels, ids

    return _parse_examples


def get_map_function_weighted(model_features, model_target, weight_name, weight_value):
    """create function for transforming data during tf data pipeline

    Args:
        model_features (list): feature definition
        model_target (nametuple): target nametuple
        weight_name (string): name of weight column in dataframe
        weight_value: if 100, then upweight would be 101

    Returns:
        function: function for transforming
    """
    feature_description = {f.name: (tf.io.FixedLenFeature(1, f.type, f.default_value) if f.max_len == 0 else tf.io.VarLenFeature(dtype=f.type)) for f
                                                     in model_features }
    feature_description[model_target.name] = tf.io.FixedLenFeature(1, model_target.type, model_target.default_value)
    feature_description[weight_name] = tf.io.FixedLenFeature(1, tf.int64, 0)

    def _parse_examples(serial_exmp):
        features = tf.io.parse_example(serial_exmp, features=feature_description)
        for f in model_features:
                    key = f.name
                    if f.max_len > 0:
                        features[key] = tf.sparse.to_dense(features[key])
        label = features.pop(model_target.name)
        weight = tf.add(tf.multiply(features.pop(weight_name), weight_value), 1)
        return features, label, weight

    return _parse_examples


def get_dcn_input(cross_num, dnn_feature_columns, dnn_hidden_units, l2_reg_embedding, l2_reg_linear,
                  linear_feature_columns, seed):
    if len(dnn_hidden_units) == 0 and cross_num == 0:
        raise ValueError("Either hidden_layer or cross layer must > 0")
    features = build_input_features(dnn_feature_columns)
    inputs_list = list(features.values())
    linear_logit = get_linear_logit(features, linear_feature_columns, seed=seed, prefix='linear',
                                    l2_reg=l2_reg_linear)
    sparse_embedding_list, dense_value_list = input_from_feature_columns(features, dnn_feature_columns,
                                                                         l2_reg_embedding, seed)
    dnn_input = combined_dnn_input(sparse_embedding_list, dense_value_list)
    return dnn_input, inputs_list, linear_logit
