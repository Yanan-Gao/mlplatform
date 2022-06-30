import math

import keras
from keras.models import Model
from keras.layers import Dense, Lambda, Input, Dropout
from kongming.preprocessing import *
from kongming.layers import add_linbndrop, LinBnDrop
import tensorflow as tf

#pre-model constructions
# generate feature input model layer
def model_input_layer(features, top_mlp_layers, dropout_p, activation, assets_path, batchnorm, pretrained_layer=None):

    if pretrained_layer==None:
        input_tuple_cat = []
        input_tuple_con = []
        for f in features:
            if f.type == tf.int64 and f.ppmethod=='simple':
                input_tuple_cat.append( int_embedding(f.name, f.cardinality, f.embedding_dim) )
            elif f.type == tf.float32:
                input_tuple_con.append(float_feature(f.name, f.type))
            elif f.type == tf.string:
                if f.ppmethod=='string_vocab' and assets_path != None:
                    input_tuple_cat.append(string_embedding(f.name, f.embedding_dim, assets_path))
                else:
                    raise Exception("Wrong preprocessing method or no asset file.")
            else:
                raise Exception("unknown input config.")

        #concat, drop for categorical
        input_cat = [x[0] for x in input_tuple_cat]
        input_cat_layer = keras.layers.Concatenate(name="cat_concat")([x[1] for x in input_tuple_cat])
        #input_cat_layer = keras.layers.Dropout(seed=42, rate=dropout_p, name=f"input_cat_dropout")(input_cat_layer)

        # concat, bn for continuous
        input_con = [x[0] for x in input_tuple_con]
        input_con_layer = keras.layers.Concatenate(name="con_concat")([x[1] for x in input_tuple_con])
        input_con_layer = keras.layers.BatchNormalization(name=f"input_con_batchnorm")(input_con_layer)

        inputs = input_cat + input_con
        output_layer = keras.layers.Concatenate(name="overall_input_layer")([input_cat_layer, input_con_layer])
        output_layer = add_linbndrop(input_layer=output_layer, add_layers=top_mlp_layers,
                                     dropout_rate=dropout_p, activation=activation)
        return inputs, output_layer
    else:
        output_layer=add_linbndrop(input_layer=pretrained_layer.output, add_layers=top_mlp_layers, dropout_rate=dropout_p, batchnorm=batchnorm)
        return pretrained_layer.input, output_layer

#generate the dimension branch layer
def model_dim_layer(
        f
        , em_size
        , dropout_p
        , assets_path
    ):
    if f.type == tf.int64 and f.ppmethod == 'simple':
        i, layer = int_embedding(f.name, f.cardinality, em_size, f.type)
    elif f.ppmethod=='string_mapping' and assets_path != None:
        i, layer = string_map_embedding(f.name, em_size, assets_path)
    else:
        raise Exception("failed to construct dim feature branch.")

    #layer = keras.layers.Dropout(seed=42, rate=dropout_p, name=f"dim_dropout")(layer)
    return i, layer

# basic model
def dot_product_model(features,
              dim_feature,
              activation="relu",
              top_mlp_layers=[256, 64],
              dropout_rate=0.3,
              batchnorm=True,
              assets_path=None,
              pretrained_layer=None
              ):
    feature_inputs, feature_layer = model_input_layer(features,
                                                          top_mlp_layers=top_mlp_layers,
                                                          dropout_p=dropout_rate,
                                                          activation=activation,
                                                          assets_path=assets_path,
                                                          batchnorm=batchnorm,
                                                          pretrained_layer=pretrained_layer

                                                          )
    dim_em_size = feature_layer.shape[1]
    dim_input, dim_layer = model_dim_layer(
        dim_feature
        , em_size=dim_em_size
        , dropout_p=dropout_rate
        , assets_path=assets_path
    )


    model_input = feature_inputs + [dim_input]

    dot_lambda = lambda array: tf.keras.layers.dot([array[0], array[1]], axes=1)

    output = Dense(1, activation='sigmoid', name="Score")(Lambda(dot_lambda, name='lambda_layer')([feature_layer, dim_layer]))


    model = Model(
        inputs=model_input,
        outputs=output,
        name="LRWithEmbedding")

    return model

# AE model to deal with bidrequests
def auto_encoder_model(features,
                          ae_units=[128,64,128],
                          dropout_rate=None,
                          batchnorm=True,
                          assets_path=None,
                          target_size=None
                       ):
    feature_inputs, feature_layer = model_input_layer(features,
                                                          top_mlp_layers=[],
                                                          dropout_p=dropout_rate,
                                                          activation="relu",
                                                          batchnorm = batchnorm,
                                                          assets_path=assets_path
                                                          )
    num_features = len(features)
    autoencoder = add_linbndrop(feature_layer, add_layers=ae_units[:math.floor(len(ae_units)/2)], batchnorm=batchnorm, dropout_rate=0.01)
    autoencoder = add_linbndrop(autoencoder, add_layers=ae_units[math.floor(len(ae_units)/2):], batchnorm=batchnorm, dropout_rate=dropout_rate)
    cont_output = LinBnDrop(target_size['cont'], batchnorm, dropout_rate, 'relu', 'cont', final_name='cont')(autoencoder)

    cat_output = Dense(target_size['cat'],  activation='sigmoid', name='cat')(
        Dropout(seed=42, rate=dropout_rate, name=f"cat_dropout")(autoencoder))

    model = Model(
        inputs=feature_inputs,
        outputs=[cont_output, cat_output],
        name="AEModel")
    return model

# model with AE pretrained
def load_pretrained_embedding(path, layer_id):
    loaded_model = tf.keras.models.load_model(path, compile=False)#custom_objects={"AELoss": AELoss})
    #chop the top and use the compressed layer of the model
    encoder = Model(loaded_model.input, loaded_model.layers[-layer_id].output)
    encoder.trainable = False
    return encoder

# multi-task learning model
# ESMM (ref: https://deepctr-doc.readthedocs.io/en/latest/_modules/deepctr/models/multitask/esmm.html)
