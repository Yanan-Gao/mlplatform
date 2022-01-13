import tensorflow as tf
import tensorflow_probability as tfp

from plutus import ModelHeads
from plutus.layers import DotInteraction, parameter_layer, get_mlp, fastai_tabular_mlp, model_input_layer, \
    emb_sparse_dense, distribution_layer

tfd = tfp.distributions
tfb = tfp.bijectors


def model_heads(all_inputs, pre_parameter_layer, distribution, multi_output_enum):
    """
    The output of the model.

    Note that if using mixed_precision training, the output should always be float32
    `Even if the model does not end in a softmax, the outputs should still be
    float32 <https://www.tensorflow.org/guide/mixed_precision#building_the_model>`_ .

    """
    if multi_output_enum == ModelHeads.CPD_FLOOR_WIN:
        floor_out = tf.keras.layers.Dense(1, activation=tf.keras.activations.linear, name="floor", dtype="float32")(pre_parameter_layer)
        win_out = tf.keras.layers.Dense(1, activation=tf.keras.activations.sigmoid, name="win", dtype="float32")(pre_parameter_layer)
        model = tf.keras.Model(inputs=all_inputs, outputs=[distribution, floor_out, win_out])

    elif multi_output_enum == ModelHeads.CPD_FLOOR:
        floor_out = tf.keras.layers.Dense(1, activation=tf.keras.activations.linear, name="floor", dtype="float32")(pre_parameter_layer)
        model = tf.keras.Model(inputs=all_inputs, outputs=[distribution, floor_out])

    elif multi_output_enum == ModelHeads.CPD or multi_output_enum == ModelHeads.REG:
        model = tf.keras.Model(inputs=all_inputs, outputs=distribution)

    else:
        raise Exception("unknown model arch")

    return model


def super_basic_model(features,
                      activation="relu",
                      combiner=tf.keras.layers.Flatten(),
                      top_mlp_layers=[512, 256, 64],
                      cpd_type=None,
                      heads=None,
                      mixture_components=2,
                      dropout_rate=None,
                      batchnorm=False
                      ):
    model_inputs, pre_parameter_layer = model_input_layer(features,
                                                  emb_combiner=combiner,
                                                  dense_bn=batchnorm,
                                                  dropout_p=dropout_rate)

    parameters = parameter_layer(pre_parameter_layer, cpd_type, mixture_components)
    distribution = distribution_layer(parameters, cpd_type)
    model = model_heads(model_inputs, pre_parameter_layer, distribution, heads)

    return model


def basic_model(features,
                activation="relu",
                combiner=tf.keras.layers.Flatten(),
                top_mlp_layers=[512, 256, 64],
                cpd_type=None,
                heads=None,
                mixture_components=2,
                dropout_rate=None,
                batchnorm=False
                ):
    model_inputs, input_layer = model_input_layer(features,
                                                  emb_combiner=combiner,
                                                  dense_bn=batchnorm,
                                                  dropout_p=dropout_rate)

    pre_parameter_layer = get_mlp(input_layer,
                         top_mlp_layers,
                         activation=activation,
                         batchnorm=batchnorm,
                         dropout_rate=dropout_rate,
                         position="top")

    parameters = parameter_layer(pre_parameter_layer, cpd_type, mixture_components)
    distribution = distribution_layer(parameters, cpd_type)

    model = model_heads(model_inputs, pre_parameter_layer, distribution, heads)

    return model


def fastai_tabular_model(features,
                         activation="relu",
                         combiner=tf.keras.layers.Flatten(),
                         layers=[1000, 500],
                         cpd_type=None,
                         heads=None,
                         mixture_components=2,
                         dropout_rate=None,
                         batchnorm=False):
    """
    https://github.com/fastai/fastai/blob/master/fastai/tabular/model.py
    Linear --> BN --> Drop
    """

    model_inputs, input_layer = model_input_layer(features,
                                                  emb_combiner=combiner,
                                                  dense_bn=batchnorm,
                                                  dropout_p=dropout_rate)

    pre_parameter_layer = fastai_tabular_mlp(input_layer=input_layer,
                                    layers=layers,
                                    activation=activation,
                                    batchnorm=batchnorm,
                                    dropout_rate=dropout_rate)

    parameters = parameter_layer(pre_parameter_layer, cpd_type, mixture_components)
    distribution = distribution_layer(parameters, cpd_type)

    model = model_heads(model_inputs, pre_parameter_layer, distribution, heads)

    return model


def dlrm_model(features,
               activation="relu",
               combiner=tf.keras.layers.Flatten(),
               bottom_mlp_layers=[512, 256, 64, 16],
               top_mlp_layers=[512, 256, 64],
               cpd_type=None,
               heads=None,
               mixture_components=2,
               dropout_rate=None,
               batchnorm=False
               ):
    model_inputs, emb_in_list, dense_in = emb_sparse_dense(features, emb_combiner=combiner, dense_bn=batchnorm)

    # if dense input then push through a MLP
    if dense_in is not None:
        b = get_mlp(dense_in,
                    bottom_mlp_layers,
                    batchnorm=batchnorm,
                    activation=activation,
                    dropout_rate=dropout_rate,
                    position="bottom")

        # add bottom mlp (this will add to interactions and to top mlp input)
        emb_in_list.append(b)

    # Interaction layer
    fm_layer = DotInteraction(self_interaction=False, name='FM')
    interactions = fm_layer(emb_in_list)

    top_input = tf.keras.layers.concatenate(emb_in_list + [interactions], name='top_input')
    top_input = tf.keras.layers.Dropout(dropout_rate, name=f"d_top_input{dropout_rate}")(
        top_input) if dropout_rate is not None else top_input

    pre_parameter_layer = get_mlp(top_input,
                         top_mlp_layers,
                         batchnorm=batchnorm,
                         activation=activation,
                         dropout_rate=dropout_rate,
                         position="top")

    parameters = parameter_layer(pre_parameter_layer, cpd_type, mixture_components)
    distribution = distribution_layer(parameters, cpd_type)
    model = model_heads(model_inputs, pre_parameter_layer, distribution, heads)

    return model


def replace_last_layer(model):
    params_layer = model.get_layer("params").output
    return tf.keras.Model(model.inputs, params_layer)
