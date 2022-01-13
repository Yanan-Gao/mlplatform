import tensorflow as tf
import tensorflow.keras.backend as K

# Custom Loss
# The function name is sufficient for loading as long as it is registered as a custom object.


def register_keras_custom_object(cls):
    tf.keras.utils.get_custom_objects()[cls.__name__] = cls
    return cls


@register_keras_custom_object
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


@register_keras_custom_object
def google_fpa_nll(y, distr):
    """
    Loss function to incorporate minimum_bid_to_win information based on details from `google docs
    <https://developers.google.com/authorized-buyers/rtb/request-guide#first-price-auctions>`_.
        If you won the auction, this will be the lowest bid you could have placed while still winning.
        If you lost the auction, this will be the winning bid.

    In both cases we will use the PDF (log_prob) to move the distribution towards the MB2W.

    We also clip the MB2W to some value in the range [epsilon, 100000]
        Hard to justify a MB2W that is very large or very small
        Preprocessing on the ETL removes some extreme values

    According to `this issue <https://github.com/tensorflow/tensorflow/issues/42446>`_ the custom loss function should
    return an array of losses, one for each element in the batch.
    """
    _imp, _bid, _mc, _mb2w, _floor = tf.unstack(y, axis=1)
    # mb2w is known for all training examples, therefore we can use log_prob 
    # guard against log(0)

    loss = -distr.log_prob(K.clip(_mb2w, K.epsilon(), 100000.0))
    return loss


@register_keras_custom_object
def google_mse_loss(y, pred):
    _imp, _bid, _mc, _mb2w, _floor = tf.unstack(y, axis=1)
    return tf.losses.mse(_mb2w, pred)


@register_keras_custom_object
def google_bce_loss(y, y_hat):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    return tf.keras.losses.binary_crossentropy(_imp, tf.squeeze(y_hat))


@register_keras_custom_object
def implicit_nll(y, distr):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    return tf.where(_imp == 1.,
                    -distr.log_cdf(_bid),
                    -distr.log_survival_function(_bid))

@register_keras_custom_object
def implicit_nll(y, distr):
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)
    return tf.where(_imp == 1.,
                    -distr.log_cdf(_bid),
                    -distr.log_survival_function(_bid))

