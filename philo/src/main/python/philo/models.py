from philo.features import build_input_features, get_linear_logit, input_from_feature_columns, SparseFeat, DenseFeat, \
    get_dcn_input
from philo.layers import add_func, concat_func, combined_dnn_input, \
    DNN, FM, PredictionLayer, CrossNet, CrossNetMix, CIN, Linear
from philo.neo import create_combined_encoder
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.initializers import glorot_normal
from tensorflow.keras.layers import Dense, Concatenate, Embedding, InputLayer
from collections import OrderedDict
from itertools import chain
import warnings

############################################################################################
# starting of ctr model
############################################################################################
DEFAULT_GROUP_NAME = "default_group"
SEED = 1024


def model_builder(model_arch, model_features, **kwargs):
    """build model

    Args:
        model_arch: model architecture, currently only implemented deep fm
        model_features (list): model feature description
        kwargs (dict): necessary parameter settings to set up model

    Returns:
        model
    """
    fixlen_feature_columns = [SparseFeat(feat.name, vocabulary_size=feat.cardinality, embedding_dim=feat.embedding_dim)
                              if feat.sparse else DenseFeat(feat.name, 1, ) for feat in model_features]
    dnn_feature_columns = fixlen_feature_columns
    linear_feature_columns = fixlen_feature_columns
    if model_arch == 'deepfm':
        model = deep_fm(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
    elif model_arch == 'deepfm_dual':
        if 'adgroup_feature_list' in kwargs.keys():
            model = deep_fm_dual_tower(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
        else:
            raise Exception("need to specify adgroup_feature_list to split features, run get_features_from_json"
                            "with get_adgroup_feature=True to get the adgroup_feature_list")
    elif model_arch == 'xdeepfm':
        model = xdeepfm(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
    elif model_arch == 'dcn':
        model = dcn(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
    elif model_arch == 'dcnv2':
        model = dcn_mix(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
    else:
        raise Exception(f"{model_arch} is not implemented yet")
    return model


def deep_fm_dual_tower(linear_feature_columns, dnn_feature_columns, adgroup_feature_list, fm_group=[DEFAULT_GROUP_NAME],
                       dnn_hidden_units=(64), l2_reg_linear=0.00001, l2_reg_embedding=0.00001,
                       l2_reg_dnn=0, seed=SEED, dnn_dropout=0,
                       dnn_activation='relu', dnn_use_bn=False, task='binary'):
    """Instantiates the DeepFM Network architecture with adgroup and bidrequest feature splitted in mlp part

    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param adgroup_feature_list: an list contains all the features belongs to adgroup
    :param fm_group: list, group_name of features that will be used to do feature interactions.
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number
                             and units in each layer of DNN
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: float. L2 regularizer strength applied to embedding vector
    :param l2_reg_dnn: float. L2 regularizer strength applied to DNN
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_activation: Activation function to use in DNN
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not in DNN
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.

    Args:
        adgroup_feature_list:
        adgroup_feature_list:
    """

    features = build_input_features(
        linear_feature_columns + dnn_feature_columns)

    inputs_list = list(features.values())

    linear_logit = get_linear_logit(features, linear_feature_columns, seed=seed, prefix='linear',
                                    l2_reg=l2_reg_linear)
    # create embedding layers
    group_embedding_dict, dense_value_list = input_from_feature_columns(features, dnn_feature_columns, l2_reg_embedding,
                                                                        seed, support_group=True)
    # Deep part of the model, which is factorization operations
    fm_logit = add_func([FM()(concat_func(v, axis=1))
                         for k, v in group_embedding_dict.items() if k in fm_group])
    # deep fm use both embedded feature and original dense features in the deep tower
    dnn_input = combined_dnn_input(list(chain.from_iterable(
        group_embedding_dict.values())), dense_value_list)
    if dnn_hidden_units:
        dual_combined_output = create_combined_encoder(group_embedding_dict, dense_value_list, adgroup_feature_list,
                                                       dnn_hidden_units=dnn_hidden_units, l2_reg_dnn=l2_reg_dnn,
                                                       seed=seed, dnn_dropout=dnn_dropout,
                                                       dnn_activation=dnn_activation, dnn_use_bn=dnn_use_bn)
    else:
        # if no list, then directly connect to a dense output, which will be equal to a linear model based on embedding
        dual_combined_output = Dense(1, use_bias=False, kernel_initializer=glorot_normal(seed=seed))(dnn_input)

    final_logit = add_func([linear_logit, fm_logit, dual_combined_output])

    output = PredictionLayer(task)(final_logit)
    model = Model(inputs=inputs_list, outputs=output)
    return model


def deep_fm(linear_feature_columns, dnn_feature_columns, fm_group=[DEFAULT_GROUP_NAME], dnn_hidden_units=(128, 128),
            l2_reg_linear=0.00001, l2_reg_embedding=0.00001, l2_reg_dnn=0, seed=SEED, dnn_dropout=0,
            dnn_activation='relu', dnn_use_bn=False, task='binary'):
    """Instantiates the DeepFM Network architecture. https://arxiv.org/abs/1703.04247

    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param fm_group: list, group_name of features that will be used to do feature interactions.
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number
                             and units in each layer of DNN
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: float. L2 regularizer strength applied to embedding vector
    :param l2_reg_dnn: float. L2 regularizer strength applied to DNN
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_activation: Activation function to use in DNN
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not in DNN
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.
    """

    features = build_input_features(
        linear_feature_columns + dnn_feature_columns)

    inputs_list = list(features.values())

    linear_logit = get_linear_logit(features, linear_feature_columns, seed=seed, prefix='linear',
                                    l2_reg=l2_reg_linear)
    # create embedding layers
    group_embedding_dict, dense_value_list = input_from_feature_columns(features, dnn_feature_columns, l2_reg_embedding,
                                                                        seed, support_group=True)
    # Deep part of the model, which is factorization operations
    fm_logit = add_func([FM()(concat_func(v, axis=1))
                         for k, v in group_embedding_dict.items() if k in fm_group])
    # deep fm use both embedded feature and original dense features in the deep tower
    dnn_input = combined_dnn_input(list(chain.from_iterable(
        group_embedding_dict.values())), dense_value_list)
    dnn_output = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
    dnn_logit = Dense(
        1, use_bias=False, kernel_initializer=glorot_normal(seed=seed))(dnn_output)

    final_logit = add_func([linear_logit, fm_logit, dnn_logit])

    output = PredictionLayer(task)(final_logit)
    model = Model(inputs=inputs_list, outputs=output)
    return model


def xdeepfm(linear_feature_columns, dnn_feature_columns, dnn_hidden_units=(256, 128, 64),
            cin_layer_size=(128, 128,), cin_split_half=True, cin_activation='relu', l2_reg_linear=0.00001,
            l2_reg_embedding=0.00001, l2_reg_dnn=0, l2_reg_cin=0, seed=1024, dnn_dropout=0,
            dnn_activation='relu', dnn_use_bn=False, task='binary'):
    """Instantiates the xDeepFM architecture.
    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number and units in each layer
           of deep net
    :param cin_layer_size: list,list of positive integer or empty list, the feature maps  in each hidden layer of
           Compressed Interaction Network
    :param cin_split_half: bool.if set to True, half of the feature maps in each hidden will connect to output unit
    :param cin_activation: activation function used on feature maps
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: L2 regularizer strength applied to embedding vector
    :param l2_reg_dnn: L2 regularizer strength applied to deep net
    :param l2_reg_cin: L2 regularizer strength applied to CIN.
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_activation: Activation function to use in DNN
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not in DNN
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.
    """

    features = build_input_features(
        linear_feature_columns + dnn_feature_columns)

    inputs_list = list(features.values())

    linear_logit = get_linear_logit(features, linear_feature_columns, seed=seed, prefix='linear',
                                    l2_reg=l2_reg_linear)

    sparse_embedding_list, dense_value_list = input_from_feature_columns(features, dnn_feature_columns,
                                                                         l2_reg_embedding, seed)

    fm_input = concat_func(sparse_embedding_list, axis=1)

    dnn_input = combined_dnn_input(sparse_embedding_list, dense_value_list)
    dnn_output = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
    dnn_logit = Dense(1, use_bias=False)(dnn_output)

    final_logit = add_func([linear_logit, dnn_logit])

    if len(cin_layer_size) > 0:
        exFM_out = CIN(cin_layer_size, cin_activation,
                       cin_split_half, l2_reg_cin, seed)(fm_input)
        exFM_logit = Dense(1, use_bias=False)(exFM_out)
        final_logit = add_func([final_logit, exFM_logit])

    output = PredictionLayer(task)(final_logit)

    model = Model(inputs=inputs_list, outputs=output)
    return model


def dcn_mix(linear_feature_columns, dnn_feature_columns, cross_num=2,
            dnn_hidden_units=(256, 128, 64), l2_reg_linear=1e-5, l2_reg_embedding=1e-5, low_rank=32, num_experts=4,
            l2_reg_cross=1e-5, l2_reg_dnn=0, seed=1024, dnn_dropout=0, dnn_use_bn=False,
            dnn_activation='relu', task='binary'):
    """Instantiates the Deep&Cross Network with mixture of experts architecture.
    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param cross_num: positive integet,cross layer number
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number and units in each layer of
                             DNN
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: float. L2 regularizer strength applied to embedding vector
    :param l2_reg_cross: float. L2 regularizer strength applied to cross net
    :param l2_reg_dnn: float. L2 regularizer strength applied to DNN
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not DNN
    :param dnn_activation: Activation function to use in DNN
    :param low_rank: Positive integer, dimensionality of low-rank space.
    :param num_experts: Positive integer, number of experts.
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.
    """
    dnn_input, inputs_list, linear_logit = get_dcn_input(cross_num, dnn_feature_columns, dnn_hidden_units,
                                                         l2_reg_embedding, l2_reg_linear, linear_feature_columns, seed)

    if len(dnn_hidden_units) > 0 and cross_num > 0:  # Deep & Cross
        deep_out = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
        cross_out = CrossNetMix(low_rank=low_rank, num_experts=num_experts, layer_num=cross_num,
                                l2_reg=l2_reg_cross)(dnn_input)
        stack_out = Concatenate()([cross_out, deep_out])
        final_logit = Dense(1, use_bias=False)(stack_out)
    elif len(dnn_hidden_units) > 0:  # Only Deep
        deep_out = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
        final_logit = Dense(1, use_bias=False, )(deep_out)
    elif cross_num > 0:  # Only Cross
        cross_out = CrossNetMix(low_rank=low_rank, num_experts=num_experts, layer_num=cross_num,
                                l2_reg=l2_reg_cross)(dnn_input)
        final_logit = Dense(1, use_bias=False, )(cross_out)
    else:  # Error
        raise NotImplementedError

    final_logit = add_func([final_logit, linear_logit])
    output = PredictionLayer(task)(final_logit)

    model = Model(inputs=inputs_list, outputs=output)

    return model


def dcn(linear_feature_columns, dnn_feature_columns, cross_num=2, cross_parameterization='vector',
        dnn_hidden_units=(256, 128, 64), l2_reg_linear=1e-5, l2_reg_embedding=1e-5,
        l2_reg_cross=1e-5, l2_reg_dnn=0, seed=1024, dnn_dropout=0, dnn_use_bn=False,
        dnn_activation='relu', task='binary'):
    """Instantiates the Deep&Cross Network architecture. added by Paniti
    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param cross_num: positive integet,cross layer number
    :param cross_parameterization: str, ``"vector"`` or ``"matrix"``, how to parameterize the cross network.
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number and units in each layer of DNN
    :param l2_reg_linear: float. L2 regularizer strength applied to linear part
    :param l2_reg_embedding: float. L2 regularizer strength applied to embedding vector
    :param l2_reg_cross: float. L2 regularizer strength applied to cross net
    :param l2_reg_dnn: float. L2 regularizer strength applied to DNN
    :param seed: integer ,to use as random seed.
    :param dnn_dropout: float in [0,1), the probability we will drop out a given DNN coordinate.
    :param dnn_use_bn: bool. Whether use BatchNormalization before activation or not DNN
    :param dnn_activation: Activation function to use in DNN
    :param task: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss
    :return: A Keras model instance.
    """
    dnn_input, inputs_list, linear_logit = get_dcn_input(cross_num, dnn_feature_columns, dnn_hidden_units,
                                                         l2_reg_embedding, l2_reg_linear, linear_feature_columns, seed)

    if len(dnn_hidden_units) > 0 and cross_num > 0:  # Deep & Cross
        deep_out = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
        cross_out = CrossNet(cross_num, parameterization=cross_parameterization, l2_reg=l2_reg_cross)(dnn_input)
        stack_out = Concatenate()([cross_out, deep_out])
        final_logit = Dense(1, use_bias=False)(stack_out)
    elif len(dnn_hidden_units) > 0:  # Only Deep
        deep_out = DNN(dnn_hidden_units, dnn_activation, l2_reg_dnn, dnn_dropout, dnn_use_bn, seed=seed)(dnn_input)
        final_logit = Dense(1, use_bias=False)(deep_out)
    elif cross_num > 0:  # Only Cross
        cross_out = CrossNet(cross_num, parameterization=cross_parameterization, l2_reg=l2_reg_cross)(dnn_input)
        final_logit = Dense(1, use_bias=False)(cross_out)
    else:  # Error
        raise NotImplementedError

    final_logit = add_func([final_logit, linear_logit])
    output = PredictionLayer(task)(final_logit)

    model = Model(inputs=inputs_list, outputs=output)

    return model
