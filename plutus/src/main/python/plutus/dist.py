import tensorflow as tf
from tensorflow.keras import backend as K
import tensorflow_probability as tfp
tfd = tfp.distributions
tfb = tfp.bijectors


def lognorm_distr(params):
    """
    Constructs a LogNormal Distribution from the given parameters.

    The parameters should come from the last layer (note: activations have to be chosen carefully)

    loc: (mu) should be the mean of the underlying distribution (i.e. if we want expected value to be around 40.1 then loc should be log(40.1).
       This value can be negative therefore we use linear activation.
       Clipped to be ( log(0.01), log(300) )

    scale: (sigma) the standard deviation of the underlying distribution.
        This should be non-negative therefore we use softplus activation
        Clipped the range to be (1e-7, 3)
    :param params:
    :return:
    """

    # loc, scale = params[:,0], params[:,1]
    loc, scale = tf.unstack(params, axis=-1)

    # mu can be negative but should be clipped to be in a reasonable range
    # sigma should be positive
    loc_clipped = K.clip(loc, K.log(0.01), K.log(300.))
    scale_clipped = K.clip(scale, K.epsilon(), 3.)

    return tfp.distributions.LogNormal(loc=loc_clipped, scale=scale_clipped)


def mixture_logistic_dist(params, num_components=3):
    """
    params = concat([mu, sigma, probs])

    mu: mean of underlying distributions
        linear activation

    sigma: standard deviations of distributions
        soft plus activation (as negative std are not allowed)

    probs: mixture coefficients for components
        softmax to range [0,1]

    """
    loc, scale, probs = tf.split(params, num_or_size_splits=num_components, axis=-1)
    return tfp.distributions.MixtureSameFamily(
      mixture_distribution=tfp.distributions.Categorical(probs=probs),
      components_distribution=tfp.distributions.Logistic(loc=loc, scale=scale)
    )
