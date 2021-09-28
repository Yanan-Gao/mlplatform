import tensorflow as tf
import tensorflow.keras.backend as K


def censored_fpa_nll(y, distr):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)

    # there is a bug in the tf.where and nan
    # need to have a safe value so that no nan is possible
    # https://github.com/tensorflow/tensorflow/pull/41775
    _vals = tf.where(_imp == 1., _market, _bid)

    _loss = tf.where(_imp == 1.,
                     -distr.log_prob(_vals),
                     -distr.log_survival_function(_vals))
    return _loss


def google_fpa_nll(y, distr):
    _imp, _bid, _mc, _mb2w, _floor = tf.unstack(y, axis=1)
    # mb2w is known for all training examples, therefore we can use log_prob 
    # guard against log(0) 
    _loss = -distr.log_prob(K.clip(_mb2w, K.epsilon(), 100000.0))
    return _loss


def google_mse_loss(y, pred):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    return tf.losses.mse(_market, pred)


def google_bce_loss(y, y_hat):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    return tf.keras.losses.binary_crossentropy(_imp, tf.squeeze(y_hat))


