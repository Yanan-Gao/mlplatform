from tensorflow.keras import backend as K
import tensorflow as tf

import tensorflow_probability as tfp
tfd = tfp.distributions
tfb = tfp.bijectors

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

#     tf.assert_greater(_loc_clipped, K.log(0.008),
#                       f"Location (mu) {_loc_clipped} should be greater or equal to {K.log(0.01)}")
#     tf.assert_greater(_scale_softplus_clipped, 1e-8,
#                       f"Scale (sigma) {_scale_softplus_clipped} should be greater or equal to {K.epsilon()}")

    return tfd.LogNormal(loc=_loc_clipped, scale=_scale_softplus_clipped)

def mixture_logistic_dist(params, num_components=3):
    loc, un_scale, logits = tf.split(params, num_or_size_splits=num_components, axis=-1)
    scale = tf.nn.softplus(un_scale)
    return tfd.MixtureSameFamily(
      mixture_distribution=tfd.Categorical(logits=logits),
      components_distribution=tfp.distributions.Logistic(loc=loc, scale=scale)
    )



def censored_fpa_nll(y, distr):
#     tf.assert_rank(y, 2)
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
#     tf.assert_rank(y, 2)
    _imp, _bid, _mc, _market, _floor = tf.unstack(y, axis=1)

    # mb2w is known for all training examples, therefore we can use log_prob
    _loss = -distr.log_prob(_market)
    return _loss
