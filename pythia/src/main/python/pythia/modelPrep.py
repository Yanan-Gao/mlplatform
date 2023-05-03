# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Functions for initializing the model and saving intermediary model weights
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Model
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

from pythia.tabnet import TabNet

def embedding(name, vocab_size=10000, emb_dim=40, dtype=tf.int32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    em = layers.Embedding(input_dim=vocab_size, output_dim=emb_dim, name=f"embedding_{name}",
                          embeddings_initializer='he_normal', )  # output shape: (None,1,emb_dim)
    f = layers.Flatten(name=f"flatten_{name}")  # flatten output shape: (None,1*emb_dim)
    dr = layers.Dropout(seed=13, rate=0.2, name=f"layer_{name}_dropout")
    return i, dr(f(em(i)))


def value_feature(name, dtype=tf.float32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    return i, i


def list_to_embedding(name, vocab_size, em_size):
    i = keras.Input(shape=(1, None), dtype=tf.int32, name=f"{name}")
    em = layers.Embedding(input_dim=vocab_size, output_dim=em_size, name=f"embedding_{name}",
                          embeddings_initializer='he_normal', )
    # embeddings_regularizer=emb_L2)
    # use for the vary length matrix
    re = layers.Reshape(target_shape=(-1, em_size), name=f"reshape_{name}")
    dr = layers.Dropout(seed=13, rate=0.2, name=f"layer_{name}_dropout")
    return i, dr(re(em(i)))


def init_model_interest(model_features, target_length, dense_layer_dim = 64, tabnet_factor = 1.0, initializer = tf.keras.initializers.HeNormal()):
    model_input_features_tuple = [embedding(f.name, f.cardinality, f.embedding_dim, f.type)
                                  if f.type == tf.int32 else value_feature(f.name)
                                  for f in model_features]
    model_input_features = [x[0] for x in model_input_features_tuple]
    model_input_layers = [x[1] for x in model_input_features_tuple]
    model_input_layers = layers.concatenate(model_input_layers)

    model_input = model_input_features

    emb_dr = layers.Dropout(seed=13, rate=0.4, name=f"imp64_dropout")
    model_input_layers1 = emb_dr(
        Dense(64, activation=None, kernel_initializer=initializer, name=f"embedding64_imp")(model_input_layers))

    if tabnet_factor > 0.0:
        tabnet = TabNet(num_features=model_input_layers.shape[-1],
                        feature_dim=int(1.3 * 64),
                        output_dim=64,
                        num_decision_steps=5)
        tab = tabnet(model_input_layers)

        output = tab * tf.constant(tabnet_factor) + model_input_layers1
        output = layers.Flatten(name="Output")(
            Dense(target_length, activation='sigmoid', kernel_initializer=initializer)(
              Dense(dense_layer_dim, activation='relu', kernel_initializer=initializer)(output)))
    else:
        output = layers.Flatten(name="Output")(
          Dense(target_length, activation='sigmoid', kernel_initializer=initializer)(
            Dense(dense_layer_dim, activation='relu', kernel_initializer=initializer)(model_input_layers1)))

    model = Model(
        inputs=model_input,
        outputs=output,  # [branch1, branch2],
        name="LRWithEmbedding")

    return model


def get_callbacks(checkpoint_filepath, patience=2, save_best=True):
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=patience, restore_best_weights=True)
    mc = ModelCheckpoint(checkpoint_filepath,
                         save_weights_only=True,
                         monitor='val_loss',
                         mode='min',
                         save_best_only=save_best)
    if save_best:
        return [es, mc]
    else:
        return [es]
