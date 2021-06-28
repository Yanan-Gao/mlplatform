import tensorflow as tf
import tensorflow.keras as keras
import tensorflow.keras.activations as activations
import tensorflow.keras.backend as K
import tensorflow.keras.layers as layers
import tensorflow_probability as tfp
from tensorflow.keras.layers.experimental import preprocessing

def get_keras_string_with_unk_category_encoder(column, vocab):
    string_lookup = preprocessing.StringLookup(num_oov_indices=1, mask_token=None, vocabulary=vocab, name=f"{column}_lookup")
    one_hot_encoder = preprocessing.CategoryEncoding(max_tokens=(len(string_lookup.get_vocabulary()) + 1), output_mode='binary', name=f"{column}_encoder")
    return string_lookup, one_hot_encoder


def lognorm_distr(params):
    """
    Constructs a LogNormal Distribution from the given parameters.

    The parameters should come from a layer and the layer should _NOT_ have an activation (i.e. linear activation)

    loc: (mu) should be the mean of the underlying distribution (i.e. if we want expected value to be around 40.1 then loc
       should be log(40.1). This value can be negative but we have it clipped to be ( log(0.01), log(300) )

    scale: (sigma) the standard deviation of the underlying distribution. This should be non-negative. To enforce this
    softplus is used and we clip the range to be (1e-7, 3)
    :param params:
    :return:
    """

    _loc = params[:, 0]
    _scale = params[:, 1]

    # mu can be negative but should be clipped to be in a reasonable range
    # sigma should be positive
    _loc_clipped = K.clip(_loc, K.log(0.01), K.log(300.))
    _scale_softplus_clipped = K.clip(K.softplus(_scale), K.epsilon(), 3.)

    tf.assert_greater(_loc_clipped, K.log(0.008),
                      f"Location (mu) {_loc_clipped} should be greater or equal to {K.log(0.01)}")
    tf.assert_greater(_scale_softplus_clipped, 1e-8,
                      f"Scale (sigma) {_scale_softplus_clipped} should be greater or equal to {K.epsilon()}")

    return tfp.distributions.LogNormal(loc=_loc_clipped, scale=_scale_softplus_clipped)


def get_ttd_dropout_model(vocabs, units=64, activation="relu"):
    # Inputs
    _all_inputs = []
    _encoded_inputs = []

    for key, vocab in vocabs.items():
        # Inputs
        _input = keras.Input(shape=(1,), name=key, dtype=tf.string)
        _lookup, _encoder = get_keras_string_with_unk_category_encoder(key, vocab)

        _all_inputs.append(_input)
        _encoded_inputs.append(_encoder(_lookup(_input)))

    _encoded_all = keras.layers.concatenate(_encoded_inputs)

    # Model
    x = layers.Dense(units, activation=activation, name="layer_1")(_encoded_all)
    x = layers.Dropout(rate=0.5)(x)
    x = layers.Dense(units, activation=activation, name="layer_2")(x)
    x = layers.Dropout(rate=0.5)(x)
    x = layers.Dense(units//2, activation=activation, name="layer_3")(x)

    # CPD
    # linear activations as the distribution will softplus the scale parameter
    _params = layers.Dense(2, activation=activations.linear, name="params")(x)
    _cpd = tfp.layers.DistributionLambda(lognorm_distr)(_params)

    return keras.Model(inputs=_all_inputs, outputs=_cpd)


def open_fpa_nll(y, distr):
    tf.assert_rank(y, 2)
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    _vals = tf.where(_imp == 1., _market, _bid)

    _loss = tf.where(_imp == 1.,
                    -distr.log_prob(_vals),
                    -distr.log_survival_function(_vals))
    return _loss


def get_sparse_input_model(units=64, activation="relu"):
    _input = keras.Input(shape=(2 ** 20,), name="input", sparse=True)

    # Model
    x = layers.Dense(units // 4, activation=activation, name="layer_1")(_input)
    x = layers.Dropout(rate=0.5)(x)
    x = layers.Dense(units, activation=activation, name="layer_2")(x)
    x = layers.Dropout(rate=0.5)(x)
    x = layers.Dense(units // 2, activation=activation, name="layer_3")(x)

    # CPD
    # linear activations as the distribution will softplus the scale parameter
    _params = layers.Dense(2, activation=activations.linear, name="params")(x)
    _cpd = tfp.layers.DistributionLambda(lognorm_distr)(_params)

    return keras.Model(inputs=_input, outputs=_cpd)


def parse_sparse(example):
    feature_description = {
        "i": tf.io.VarLenFeature(tf.int64),
        "v": tf.io.VarLenFeature(tf.float32),
        "s": tf.io.FixedLenFeature([], tf.int64),
        "is_imp": tf.io.FixedLenFeature([], tf.float32),
        "mb2w": tf.io.FixedLenFeature([], tf.float32),
        "RealMediaCost": tf.io.FixedLenFeature([], tf.float32, default_value=-1.0),
        "FloorPriceInUSD": tf.io.FixedLenFeature([], tf.float32, default_value=0.0),
        "AuctionBidPrice": tf.io.FixedLenFeature([], tf.float32)
    }
    example = tf.io.parse_example(example, feature_description)
    data = tf.sparse.SparseTensor(
        indices=tf.expand_dims(tf.sparse.to_dense(example['i']), -1),  # need to expand dimensions to get N,1
        values=tf.sparse.to_dense(example['v']),
        dense_shape=tf.expand_dims(example['s'], -1)  # expects an array
    )

    targets = tf.stack(
        [
            tf.cast(example['is_imp'], tf.float32),
            example["AuctionBidPrice"],
            example["RealMediaCost"],
            example["mb2w"],
            example["FloorPriceInUSD"]
        ], axis=-1)
    return (data, targets)



