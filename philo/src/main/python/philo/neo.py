# Created by jiaxing.pi at 12/9/22

# Enter description here
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, InputLayer, Lambda

import warnings
from philo.layers import add_func, Linear, FM, PredictionLayer, combined_dnn_input, DNN
from philo.features import DenseFeat, get_linear_logit, concat_func
from philo.utils import align_model_feature
from philo.inputs import get_dense_input
from collections import OrderedDict
from itertools import chain

SEED = 1024


def extract_neo_model(model, model_arch, model_features, linear_feature_columns, adgroup_feature_name, l2_reg_linear,
                      seed=SEED):
    """
    Extract adgroup and bidrequest neo model from the factorization machine part of deepfm
    Args:
        model: original trained or untrained model
        model_arch: the architecture of the input model, currently supports 'deepfm' and 'deepfm_dual'
        model_features: model feature list, which is the input from model_builder
        linear_feature_columns: linear feature columns for the linear interaction
        adgroup_feature_name: features for the adgroup neo model
        l2_reg_linear: l2 regularization for the dense linear model
        seed: random seed

    Returns: neo model for adgroup and bidrequest

    """
    input_layers = list(filter(lambda x: isinstance(x, InputLayer), model.layers))
    if len(input_layers) != len(model_features):
        # sanity check for whether model_feature has the same length of input_layer
        raise Exception("Loaded model input dimension is different from the input dimension")
    # extract sparse embedding layer, linear embedding has output_dim==1
    embed_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim > 1, model.layers))
    one_hot_list = list(filter(lambda x: isinstance(x, Embedding) and x.output_dim == 1, model.layers))
    # make sure that model_feature has the same order of the input layer
    linear_feature_columns = align_model_feature(input_layers, linear_feature_columns)
    a_linear_feature_columns, b_linear_feature_columns = separate_feature(linear_feature_columns,
                                                                          adgroup_feature_list=adgroup_feature_name)
    a_embed_list, b_embed_list = separate_feature(embed_list, adgroup_feature_list=adgroup_feature_name)
    a_one_hot, b_one_hot = separate_feature(one_hot_list,
                                            adgroup_feature_list=adgroup_feature_name)
    a_input_layers, b_input_layers = separate_feature(input_layers, adgroup_feature_list=adgroup_feature_name)
    if model_arch == "deepfm_dual":
        a_dnn_layer = list(filter(lambda x: x.name == 'adgroup_dnn', model.layers))[0]
        b_dnn_layer = list(filter(lambda x: x.name == 'bidrequest_dnn', model.layers))[0]
    elif model_arch == "deepfm":
        a_dnn_layer, b_dnn_layer = None, None
    else:
        raise Exception("Model arch is unknown, current options are: deepfm and deepfm_dual.")

    linear_layer = list(filter(lambda x: isinstance(x, Linear), model.layers))[0]
    linear_weight = linear_layer.get_weights()
    use_bias = linear_layer.use_bias
    bias = None
    if len(linear_weight) > 1:
        bias = [linear_weight[0]]
        linear_weight = [linear_weight[1]]
    a_neo = build_neo_model(a_input_layers, a_linear_feature_columns, a_one_hot, l2_reg_linear, a_embed_list,
                            dnn_layer=a_dnn_layer, use_bias=use_bias, bias=bias, seed=seed)
    # bidrequest contains continuous features, therefore, need to load the linear weight to it.
    # align_model_feature make sure the weights are loaded to the right features
    b_neo = build_neo_model(b_input_layers, b_linear_feature_columns, b_one_hot, l2_reg_linear, b_embed_list,
                            dnn_layer=b_dnn_layer, linear_weights=linear_weight, seed=seed)
    return a_neo, b_neo


def build_neo_model(input_layers, linear_feature_columns, one_hot_layers, l2_reg_linear, embed_list,
                    dnn_layer=None, use_bias=False, bias=None, linear_weights=None, seed=SEED):
    """
    build Neo model
    Args:
        input_layers: list of keras InputLayer
        linear_feature_columns: feature list for the linear input, each element is the original model_builder input
        one_hot_layers: one hot embedding layers
        l2_reg_linear: l2 regularization for the linear dense part
        embed_list: list of embedding layers
        dnn_layer: one of the DNN layers from dual tower, depending on adgroup or bidrequest
        bias: if use bias, the value for bias, in the format of [array([float])]
        use_bias: use bias or not
        linear_weights: weights for the dense feature linear kernel, if all sparse feature, it shall be set to None
                        in the format of [array([[float], [float], ...])]
        seed: random seed

    Returns: a neo model for adgroup or bidrequest

    """
    # sanity check for the existence of dense feature, if there are dense feature but no weights, the model results
    # will be different, if there are no dense feature, sparse feature will have the weights on its 1 dimension
    # embedding, therefore, the output will be the same, but a warning will be printed
    dense_existence = any(isinstance(x, DenseFeat) for x in linear_feature_columns)
    if not dense_existence and linear_weights:
        warnings.warn("There are no dense features, therefore, linear_weights cannot be set, setting it to None")
        linear_weights = None
    elif dense_existence and not linear_weights:
        raise Exception(
            "There are dense features, but no info for the linear weight, need to pass in value so the model could be "
            "aligned")
    # dict is the only input that get_linear_logit could take
    features = OrderedDict({i.name: i.output for i in input_layers})
    linear_logit = build_linear_logit(one_hot_layers, features, linear_feature_columns, use_bias, seed, l2_reg_linear)
    # if no dense feature, the linear logit will generate a vector of (None, 1, 1)
    if not dense_existence:
        linear_logit = tf.squeeze(linear_logit, axis=-1)
    fm_vector, fm_float = get_partial_fm(embed_list)
    if dnn_layer:
        final_outputs = [linear_logit, fm_float, fm_vector, dnn_layer.output]
    else:
        final_outputs = [linear_logit, fm_float, fm_vector]
    model = Model(inputs=list(features.values()), outputs=final_outputs)

    if linear_weights and use_bias:
        weights = linear_weights + bias
    elif linear_weights:
        weights = linear_weights
    elif use_bias:
        weights = bias
    else:
        # if not linear_weights and not use_bias, just return the model
        return model
    try:
        linear_layer = list(filter(lambda x: isinstance(x, Linear), model.layers))[0]
        linear_layer.set_weights(weights)
    except ValueError as e:
        print(e)
    return model


def recalibrate_model(model, task='binary', freeze=True):
    """
    Re-construct the prediction layer of the model so that the model only calibrates the output from the factorization
    machine and linear operation.
        Args:
            model: original trained model
            task: ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
            freeze: whether to freeze layers depending on if we want to keep the weights

        Returns: model with new calibration layer

    """
    # freeze the original trained model
    model.trainable = not freeze

    input_layers = list(filter(lambda x: isinstance(x, InputLayer), model.layers))
    linear_layer = list(filter(lambda x: isinstance(x, Linear), model.layers))[0]
    fm_layer = list(filter(lambda x: isinstance(x, FM), model.layers))[0]

    # if all sparse, linear_layer output will be a vector
    if len(linear_layer.output.shape) > 2:
        linear_output = tf.squeeze(linear_layer.output, axis=-1)
    else:
        linear_output = linear_layer.output

    final_logit = add_func([linear_output, fm_layer.output])
    output = PredictionLayer(task)(final_logit)
    new_model = Model(inputs=[i.output for i in input_layers], outputs=output)
    return new_model


def build_linear_logit(one_hot_list, features, linear_feature_columns, use_bias, seed, l2_reg):
    """
    extracted from get_linear_logit for the purpose of reuse linear embedding in neo
    Args:
        one_hot_list: list of one hot layers from previously trained model
        features: OrderedDict of feature inputs logit
        linear_feature_columns: feature columns for linear, in the format of SparseFeat, DenseFeat or VarLenFeat
        use_bias: use bias in linear op or not
        seed: random seed
        l2_reg: l2 regularization for linear op

    Returns: linear logit

    """
    sparse_input = [i.output for i in one_hot_list]
    dense_input_list = get_dense_input(features, linear_feature_columns)
    if one_hot_list and dense_input_list:
        mode = 2
        sparse_input = concat_func(sparse_input)
        dense_input = concat_func(dense_input_list)
        linear_input = [sparse_input, dense_input]
    elif sparse_input:
        mode = 0
        linear_input = concat_func(sparse_input)
    elif dense_input_list:
        mode = 1
        linear_input = concat_func(dense_input_list)
    else:
        return Lambda(lambda x: tf.constant([[0.0]]))(list(features.values())[0])
    return Linear(l2_reg, mode=mode, use_bias=use_bias, seed=seed)(linear_input)


def get_partial_fm(embed_list):
    """
    get vector part for neo sum, square and sum operation, float part of square and sum operation
    Args:
        embed_list: list of embedding layers

    Returns: fm vector part and float part
    Returns: fm vector part and float part

    """
    embed_logit = [i.output for i in embed_list]
    embedding_input = concat_func(embed_logit, axis=1)
    # original shall be sum, square then sum, here vectors from bidrequest or adgroups are summed first, bid cache need
    # to sum it with the other part, then square and sum
    fm_vector = tf.reduce_sum(embedding_input, axis=1, keepdims=True)
    # this part is originally square and sum
    fm_float = tf.reduce_sum(
        tf.reduce_sum(
            embedding_input * embedding_input, axis=1, keepdims=True),
        axis=2, keepdims=False)
    return fm_vector, fm_float


def extract_fm_model(model):
    """
    Extract factorization machine + linear operation from model
    Args:
        model: factorization model that output fm logit and linear logit

    Returns: fm model

    """
    input_layers = list(filter(lambda x: isinstance(x, InputLayer), model.layers))
    linear_layer = list(filter(lambda x: isinstance(x, Linear), model.layers))[0]
    fm_layer = list(filter(lambda x: isinstance(x, FM), model.layers))[0]
    linear_output = linear_layer.output
    # if all sparse, it will be a vector
    if len(linear_layer.output.shape) > 2:
        linear_output = tf.squeeze(linear_layer.output, axis=-1)
    fm_model = Model(inputs=[i.output for i in input_layers], outputs=[linear_output, fm_layer.output])
    return fm_model


def combine_neo_results(a_neo_predict, b_neo_predict, combined_value=True):
    """
    combine neo prediction from adgroup and bidrequest
    Args:
        a_neo_predict: tuple that could be unzipped to a_linear_logit, a_fm_float, a_fm_vector
        b_neo_predict: tuple that could be unzipped to b_linear_logit, b_fm_float, b_fm_vector
        combined_value: combine fm output and linear output or not

    Returns: if combined_value, return a float of final result, if not, return fm output and linear output for testing
             purpose

    """
    pred_tuple_len = len(a_neo_predict)
    if pred_tuple_len == 4:
        a_linear_logit, a_fm_float, a_fm_vector, a_dnn_vector = a_neo_predict
        b_linear_logit, b_fm_float, b_fm_vector, b_dnn_vector = b_neo_predict
        dnn_logit_op = np.sum(a_dnn_vector * b_dnn_vector, axis=1, keepdims=True)
    elif pred_tuple_len == 3:
        a_linear_logit, a_fm_float, a_fm_vector = a_neo_predict
        b_linear_logit, b_fm_float, b_fm_vector = b_neo_predict
    else:
        raise Exception("Neo pred output dimension is unknown, should be 3 if 'fm' and 4 if 'dual_tower'.")

    sum_vector = a_fm_vector + b_fm_vector
    # could also use sum_vector * sum_vector to replace np.square
    vector_op = np.sum(np.square(sum_vector), axis=-1, keepdims=False)
    fm_vector_op = 0.5 * (vector_op - a_fm_float - b_fm_float)
    linear_logit_op = a_linear_logit + b_linear_logit

    if combined_value and pred_tuple_len == 4:
        return linear_logit_op + fm_vector_op + dnn_logit_op
    elif combined_value and pred_tuple_len == 3:
        return linear_logit_op + fm_vector_op
    else:
        return linear_logit_op, fm_vector_op


def predict_neo_results(a_neo_predict, b_neo_predict, pred_layer_weight):
    """
    Output calibrated model output based on neo predictions from adgroup and bidrequest and prediction layer weight.
    Args:
        a_neo_predict: tuple that could be unzipped to a_linear_logit, a_fm_float, a_fm_vector
        b_neo_predict: tuple that could be unzipped to b_linear_logit, b_fm_float, b_fm_vector
        pred_layer_weight: prediction layer weight from existing trained model

    Returns: model prediction

    """
    neo_results_combined = combine_neo_results(a_neo_predict, b_neo_predict, combined_value=True)
    # note that although the name weight here, the value itself actually serves as a bias term
    pred_input = neo_results_combined + pred_layer_weight
    # sigmoid function
    pred_output = 1 / (1 + np.exp(-pred_input))
    return pred_output


def separate_feature(layer_list,
                     adgroup_feature_list=['AdGroupId', 'AdvertiserId', 'CreativeId']
                     ):
    """
    Separate features into adgroup and bidrequest
    Args:
        layer_list: keras layer list from original model
        adgroup_feature_list: adgroup feature list

    Returns: two group of keras layers

    """
    adgroup_info = []
    bidrequest_info = []
    for i in layer_list:
        if any(x in i.name for x in adgroup_feature_list):
            adgroup_info.append(i)
        else:
            bidrequest_info.append(i)
    return adgroup_info, bidrequest_info


def create_combined_encoder(group_embedding_dict, dense_value_list, adgroup_feature_list, dnn_hidden_units=[64],
                            l2_reg_dnn=0, seed=SEED, dnn_dropout=0,
                            dnn_activation='relu', dnn_use_bn=False):
    sparse_feature_embeddings = list(chain.from_iterable(group_embedding_dict.values()))
    adgroup_embed, bid_embed = separate_feature(sparse_feature_embeddings, adgroup_feature_list)
    adgroup_dense, bid_dense = separate_feature(dense_value_list, adgroup_feature_list)
    adgroup_input = combined_dnn_input(adgroup_embed, adgroup_dense)
    bid_input = combined_dnn_input(bid_embed, bid_dense)
    dnn_output_adgroup = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed,
                             name="adgroup_dnn")(adgroup_input)
    dnn_output_bid = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed,
                         name="bidrequest_dnn")(bid_input)
    return tf.reduce_sum(dnn_output_adgroup * dnn_output_bid, axis=1, keepdims=True)
