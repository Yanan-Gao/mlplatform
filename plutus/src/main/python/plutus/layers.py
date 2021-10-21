from typing import List, Optional

import tensorflow as tf
import tensorflow_probability as tfp
from tensorflow.keras import activations

from . import CpdType
from .dist import lognorm_distr, mixture_logistic_dist
from .embeddings import qr_embedding, int_embedding

tfd = tfp.distributions
tfb = tfp.bijectors

DEFAULT_MLP_LAYER_UNITS = [512, 256, 64]
DEFAULT_FASTAI_TABULAR_MLP_LAYER_UNITS = [1000, 500]


class DotInteraction(tf.keras.layers.Layer):
    """Dot interaction layer.
    
    From: https://www.tensorflow.org/recommenders/api_docs/python/tfrs/layers/feature_interaction/DotInteraction
    
    See theory in the DLRM paper: https://arxiv.org/pdf/1906.00091.pdf,
    section 2.1.3. Sparse activations and dense activations are combined.
    Dot interaction is applied to a batch of input Tensors [e1,...,e_k] of the
    same dimension and the output is a batch of Tensors with all distinct pairwise
    dot products of the form dot(e_i, e_j) for i <= j if self self_interaction is
    True, otherwise dot(e_i, e_j) i < j.
    Attributes:
      self_interaction: Boolean indicating if features should self-interact.
        If it is True, then the diagonal enteries of the interaction matric are
        also taken.
      name: String name of the layer.
    """

    def __init__(self,
                 self_interaction: bool = False,
                 **kwargs) -> None:
        self._self_interaction = self_interaction
        super().__init__(**kwargs)

    @tf.function(experimental_relax_shapes=True)
    def call(self, inputs: List[tf.Tensor]) -> tf.Tensor:
        """Performs the interaction operation on the tensors in the list.
        The tensors represent as transformed dense features and embedded categorical
        features.
        Pre-condition: The tensors should all have the same shape.
        Args:
          inputs: List of features with shape [batch_size, feature_dim].
        Returns:
          activations: Tensor representing interacted features.
        """
        batch_size = tf.shape(inputs[0])[0]
        # concat_features shape: B,num_features,feature_width
        try:
            concat_features = tf.stack(inputs, axis=1)
        except (ValueError, tf.errors.InvalidArgumentError) as e:
            raise ValueError(f"Input tensors` dimensions must be equal, original error message: {e}")
        
        # Interact features, select lower-triangular portion, and re-shape.
        xactions = tf.matmul(concat_features, concat_features, transpose_b=True)
        ones = tf.ones_like(xactions)
        feature_dim = xactions.shape[-1]
        if self._self_interaction:
            # Selecting lower-triangular portion including the diagonal.
            lower_tri_mask = tf.linalg.band_part(ones, -1, 0)
            out_dim = feature_dim * (feature_dim + 1) // 2
        else:
            # Selecting lower-triangular portion not included the diagonal.
            upper_tri_mask = tf.linalg.band_part(ones, 0, -1)
            lower_tri_mask = ones - upper_tri_mask
            out_dim = feature_dim * (feature_dim - 1) // 2
        activations = tf.boolean_mask(xactions, lower_tri_mask)
        activations = tf.reshape(activations, (batch_size, out_dim))
        return activations

    def get_config(self):
        return {"self_interaction": self._self_interaction }

    @classmethod
    def from_config(cls, config):
        return cls(**config)



class LogNormLayer(tf.keras.layers.Layer):
    """
    LogNorm Layer

    Expects Params as input.

    Need to create a layer:
    https://github.com/tensorflow/probability/issues/1350

    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def call(self, inputs, training=False):
        output = tfp.layers.DistributionLambda(lognorm_distr, name='lognorm')(inputs)
        return output

    def get_config(self):
        config = dict()
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)


class MixtureLayer(tf.keras.layers.Layer):
    """
    Mixture Layer

    Expects params as input

    Need to create a layer:
    https://github.com/tensorflow/probability/issues/1350

    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def call(self, inputs, training=False):
        output = tfp.layers.DistributionLambda(mixture_logistic_dist, name='lognorm')(inputs)
        return output

    def get_config(self):
        config = dict()
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)


class ParamsLayer(tf.keras.layers.Layer):
    """
    Params layer needed as we chop off the Distribution Layer for production

    Need to create a layer:
    https://github.com/tensorflow/probability/issues/1350

    """
    def __init__(self, num_components=1, **kwargs):
        super().__init__(**kwargs)
        self.num_components = num_components
        self.mu = tf.keras.layers.Dense(self.num_components, activation=tf.keras.activations.linear, name="params_mu")
        self.sigma = tf.keras.layers.Dense(self.num_components, activation=tf.keras.activations.softplus, name="params_sigma")

        if self.num_components > 1:
            self.mixture = tf.keras.layers.Dense(self.num_components, activation=tf.keras.activations.softmax, name="params_mixture")

    def call(self, inputs):
        mu = self.mu(inputs)
        sigma = self.sigma(inputs)
        if self.num_components > 1:
            mixture = self.mixture(inputs)
            params = tf.keras.layers.concatenate([mu, sigma, mixture], name="params")
        else:
            params = tf.keras.layers.concatenate([mu, sigma], name="params")

        return params

    def get_config(self):
        config = {"num_components": self.num_components}
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)


def model_input_layer(model_features, emb_combiner, dense_bn=True, dropout_p=None):
    model_inputs, sparse, dense = emb_sparse_dense(model_features, emb_combiner, dense_bn)

    # if multiple sparse inputs, concat here
    e = tf.keras.layers.concatenate(sparse, name="sparse_emb_in") if len(sparse) > 1 else sparse[0]

    # if sparse embedding dropout specified, apply here
    e = tf.keras.layers.Dropout(dropout_p, name=f"sparse_emb_in_drop_{dropout_p}")(e) if dropout_p is not None else e

    # if dense input concat with sparse embedded input
    input_layer = tf.keras.layers.concatenate([e, dense]) if dense is not None else e

    return model_inputs, input_layer


def emb_sparse_dense(features, emb_combiner, dense_bn=True):
    model_inputs = []
    dense_inputs = []
    sparse = []

    for f in features:
        if f.sparse:
            if f.qr_embed:
                i, e = qr_embedding(f.name, vocab_size=f.cardinality, emb_dim=f.embedding_dim,
                                    num_collisions=f.qr_collisions, feat_dtype=f.type)
                sparse.append(emb_combiner(e(i)))
            else:
                i, e = int_embedding(f.name, vocab_size=f.cardinality, emb_dim=f.embedding_dim, dtype=f.type)
                sparse.append(emb_combiner(e(i)))
        else:
            i = tf.keras.Input(shape=(1,), dtype=f.type, name=f.name)
            dense_inputs.append(i)

        model_inputs.append(i)

    if len(dense_inputs) > 0:
        dense = tf.keras.layers.concatenate(dense_inputs, name='dense_in')
        dense = tf.keras.layers.BatchNormalization(name="dense_in_bn")(dense) if dense_bn else dense
        return model_inputs, sparse, dense
    else:
        return model_inputs, sparse, None


def get_mlp(input_layer, layers=DEFAULT_MLP_LAYER_UNITS, activation="relu", batchnorm=False, dropout_rate=None, position="top"):
    for i, layer in enumerate(layers):
        if i == 0:
            x = tf.keras.layers.BatchNormalization(name=f"{position}_{i}_bn")(input_layer) if batchnorm else input_layer
        else:
            x = tf.keras.layers.Dense(layer, activation=activation, name=f"{position}_{i}_dense_{layer}")(x)
            x = tf.keras.layers.Dropout(dropout_rate, name=f"{position}_{i}_drop_{dropout_rate}")(x) if dropout_rate is not None else x
    return x


def fastai_tabular_mlp(input_layer, layers=DEFAULT_FASTAI_TABULAR_MLP_LAYER_UNITS, activation="relu", batchnorm=True, dropout_rate=None):
    for i, units in enumerate(layers):
        x = lin_bn_drop(input_layer if i == 0 else x,
                        units=units,
                        layer=i,
                        bn=batchnorm,
                        p=dropout_rate if dropout_rate else 0.0
                        )

    return x


def lin_bn_drop(input_layer, layer, units, bn=True, p=0.0):
    x = tf.keras.layers.Dense(units, activation=tf.keras.activations.relu, use_bias=not bn, name=f"layer_{layer}_{units}_dense")(input_layer)
    x = tf.keras.layers.BatchNormalization(name=f"layer_{layer}_{units}_bn")(x) if bn else x
    x = tf.keras.layers.Dropout(rate=p, seed=42, name=f"layer_{layer}_{units}_drop_{p}")(x) if p != 0.0 else x
    return x


def output_layer(last_layer, cpd_type, mixture_components=1):
    if cpd_type == CpdType.LOGNORM:
        params = ParamsLayer(name="params")(last_layer)
        return LogNormLayer()(params)
    elif cpd_type == CpdType.MIXTURE:
        params = ParamsLayer(mixture_components, name="params")(last_layer)
        return MixtureLayer()(params)
    else:
        # Regression output
        return tf.keras.layers.Dense(1, activation=tf.keras.activations.linear, name="output")(last_layer)



