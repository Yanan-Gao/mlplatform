from collections import namedtuple
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models, layers, activations
from tensorflow.python.keras.utils import tf_utils

class GPUCompatibleEmbedding(layers.Embedding):
    @tf_utils.shape_type_conversion
    def build(self, input_shape):
        self.embeddings = self.add_weight(
            shape=(self.input_dim, self.output_dim),
            initializer=self.embeddings_initializer,
            name="embeddings",
            regularizer=self.embeddings_regularizer,
            constraint=self.embeddings_constraint,
        )
        self.built = True


class QREmbedding(keras.layers.Layer):
    """
    QR Trick embedding

    Similar to the Keras example but I think they have a mistake.

    The input is expected to be an integer that comes from the hash of the categorical value, modulo some
    max_value.

    The number of collisions is a hyper parameter that allows us to trade off between embedding size and collision rate.

    """

    def __init__(self,
                 vocab_size,
                 embedding_dim,
                 num_collisions=4,
                 do_hashing=False,
                 name=None):
        super(QREmbedding, self).__init__(name=name)

        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.num_buckets = num_collisions

        self.do_hashing = do_hashing

        if self.do_hashing:
            self.hasher = keras.layers.experimental.preprocessing.Hashing(num_bins=vocab_size, dtype=tf.string,
                                                                          name=f"{name}_hasher")

        self.num_embeddings = {
            "q": np.ceil(vocab_size / num_collisions).astype(int),
            "r": num_collisions
        }

        self.q_embeddings = layers.Embedding(self.num_embeddings['q'], embedding_dim)
        self.r_embeddings = layers.Embedding(self.num_embeddings['r'], embedding_dim)

    def call(self, inputs):
        idx = self.hasher(inputs) if self.do_hashing else inputs

        # Get the quotient index.
        quotient_index = tf.math.floordiv(idx, self.num_buckets)

        # Get the reminder index.
        remainder_index = tf.math.floormod(idx, self.num_buckets)

        # Lookup the quotient_embedding using the quotient_index.
        quotient_embedding = self.q_embeddings(quotient_index)

        # Lookup the remainder_embedding using the remainder_index.
        remainder_embedding = self.r_embeddings(remainder_index)

        # Use multiplication as a combiner operation
        return quotient_embedding * remainder_embedding


def qr_embedding(column, vocab_size=15, emb_dim=5, num_collisions=4, feat_dtype=tf.int64):
    """
    This deals with pre-hashed ints (done with xxhash or similar in spark/bidder)
    """

    i = keras.Input(shape=(1,), name=column, dtype=feat_dtype)

    if feat_dtype == tf.int64:
        em = QREmbedding(vocab_size=vocab_size, embedding_dim=emb_dim, num_collisions=num_collisions,
                         name=f"{column}_qr_embedding")
    elif feat_dtype == tf.string:
        em = QREmbedding(vocab_size=vocab_size, embedding_dim=emb_dim, num_collisions=num_collisions,
                         name=f"{column}_qr_embedding", do_hashing=True)
    else:
        raise Exception("only accept int or string types")

    return i, em



def hashing_embedding(column, vocab_size=15, emb_dim=5, dtype=tf.string):
    i = keras.Input(shape=(1,), name=column, dtype=dtype)
    h = keras.layers.experimental.preprocessing.Hashing(num_bins=vocab_size, name=f"{column}_hasher")
    em = GPUCompatibleEmbedding(input_dim=h.num_bins, output_dim=emb_dim, name=f"{column}_embedding")

    return i, h, em


def int_embedding(column, vocab_size, emb_dim=5):
    i = keras.Input(shape=(1,), name=column, dtype=tf.int32)
    em = GPUCompatibleEmbedding(input_dim=vocab_size, output_dim=emb_dim, name=f"{column}_embedding")

    return i, em


def hashing_encoder(column, vocab_size=15):
    i = keras.Input(shape=(1,), name=column, dtype=tf.string)
    h = keras.layers.experimental.preprocessing.Hashing(num_bins=vocab_size, name=f"{column}_hasher")
    ec = keras.layers.experimental.preprocessing.CategoryEncoding(max_tokens=vocab_size, name=f"{column}_encoder",
                                                                  output_mode="binary")
    return i, h, ec
