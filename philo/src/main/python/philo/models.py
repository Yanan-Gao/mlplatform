import tensorflow as tf
# ctr
from philo.features import build_input_features, get_linear_logit, input_from_feature_columns, SparseFeat, DenseFeat
from philo.layers import add_func, concat_func, combined_dnn_input, DNN, FM, PredictionLayer

from tensorflow.keras.models import Model
from tensorflow.keras.initializers import glorot_normal
from tensorflow.keras.layers import Dense
from itertools import chain

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
        kwargs (dict): necessary parameter settings to setup model

    Returns:
        model
    """
    fixlen_feature_columns = [SparseFeat(feat.name, vocabulary_size=feat.cardinality, embedding_dim=feat.embedding_dim)
                              if feat.sparse else DenseFeat(feat.name, 1, ) for feat in model_features]
    dnn_feature_columns = fixlen_feature_columns
    linear_feature_columns = fixlen_feature_columns
    if model_arch == 'deepfm':
        model = deep_fm(linear_feature_columns, dnn_feature_columns, task='binary', **kwargs)
    else:
        raise Exception(f"{model_arch} is not implemented yet")
    return model


def deep_fm(linear_feature_columns, dnn_feature_columns, fm_group=[DEFAULT_GROUP_NAME], dnn_hidden_units=(128, 128),
            l2_reg_linear=0.00001, l2_reg_embedding=0.00001, l2_reg_dnn=0, seed=SEED, dnn_dropout=0,
            dnn_activation='relu', dnn_use_bn=False, task='binary'):
    """Instantiates the DeepFM Network architecture. https://arxiv.org/abs/1703.04247

    :param linear_feature_columns: An iterable containing all the features used by linear part of the model.
    :param dnn_feature_columns: An iterable containing all the features used by deep part of the model.
    :param fm_group: list, group_name of features that will be used to do feature interactions.
    :param dnn_hidden_units: list,list of positive integer or empty list, the layer number and units in each layer of DNN
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
