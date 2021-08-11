from tensorflow.keras import models, layers, activations
from lib.embeddings import qr_embedding, int_embedding
from lib.dotinteractions import DotInteraction
from lib.tfutils import lognorm_distr

from tensorflow import keras
from tensorflow.keras import models, layers, activations
import tensorflow_probability as tfp


def get_full_model(features,
                   embedding_dim=16,
                   activation="relu",
                   combiner=layers.Flatten(),  # layers.GlobalMaxPool1D(),
                   qr_collisions=None
                   ):
    # Inputs
    all_inputs = []
    encoded_inputs = []

    #     for feature, (embed, feat_dtype, feat_dim) in features.items():
    for f in features:

        if f.qr_embed:
            i, e = qr_embedding(f.name, vocab_size=f.cardinality, emb_dim=f.embedding_dim,
                                num_collisions=f.qr_collisions, feat_dtype=f.type)
            encoded_inputs.append(combiner(e(i)))
        else:
            i, e = int_embedding(f.name, vocab_size=f.cardinality, emb_dim=f.embedding_dim)
            encoded_inputs.append(combiner(e(i)))

        all_inputs.append(i)

    fm_layer = DotInteraction(self_interaction=False)

    interactions = fm_layer(encoded_inputs)

    encoded_all = keras.layers.concatenate(encoded_inputs + [interactions])

    x = layers.Dense(512, activation=activation, name="top_mlp_512")(encoded_all)
    x = layers.Dense(256, activation=activation, name="top_mlp_256")(x)
    x = layers.Dense(64, activation=activation, name="top_mlp_64")(x)

    # linear activations as the distribution will softplus the scale parameter
    params = layers.Dense(2, activation=keras.activations.linear, name="params")(x)

    # CPD
    cpd = tfp.layers.DistributionLambda(lognorm_distr)(params)

    model = keras.Model(inputs=all_inputs, outputs=cpd)

    return model
