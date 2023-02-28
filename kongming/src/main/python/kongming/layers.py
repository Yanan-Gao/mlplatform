import tensorflow as tf


# `VocabLookup` is a subclass of `tf.keras.layers.Layer` that takes a string tensor as input and returns a string tensor
# as output
# its a layer to convert an input to output according to lookup table.
class VocabLookup(tf.keras.layers.Layer):
    def __init__(self, vocab_path, key_dtype=tf.string, value_dtype=tf.string, name=None):
        super(VocabLookup, self).__init__(trainable=False, name=name)
        self.vocab_path = vocab_path
        self.key_dtype = key_dtype
        self.value_dtype = value_dtype

    def build(self, input_shape):
        table_init = tf.lookup.TextFileInitializer( filename=self.vocab_path,
                                                    key_dtype=self.key_dtype,
                                                    key_index=0,
                                                    value_dtype=self.value_dtype,
                                                    value_index=1,
                                                    delimiter=",")
        if self.value_dtype==tf.string:
            self.table = tf.lookup.StaticHashTable(table_init, 'UNK')
        elif self.value_dtype==tf.float32:
            self.table = tf.lookup.StaticHashTable(table_init, 0.0)
        elif self.value_dtype==tf.int64:
            self.table = tf.lookup.StaticHashTable(table_init, 0)
        else:
            raise Exception("unknown lookup table value type.")
        self.built = True

    def call(self, input_text):
        #splitted_text = tf.strings.split(input_text).to_tensor()
        word_ids = self.table.lookup(input_text)
        return word_ids

    def get_config(self):
        config = super(VocabLookup, self).get_config()
        config.update({'vocab_path': self.vocab_path})
        return config


class LinBnDrop(tf.keras.layers.Layer):
    def __init__(self, n_out, bn=True, p=0., activation=tf.keras.activations.relu, in_name="", final_name=None):
        super().__init__(name=final_name)
        self.lin = tf.keras.layers.Dense(
            n_out, activation=activation, use_bias=not bn, name=f"layer_{in_name}_dense")
        self.bnl = tf.keras.layers.BatchNormalization(name=f"layer_{in_name}_batchnorm")
        self.drop = tf.keras.layers.Dropout(seed=42, rate=p, name=f"layer_{in_name}_dropout")
        self.bn=bn

    def call(self, inputs):
        if self.bn:
            x = self.bnl(inputs)
            x = self.drop(x)
        else:
            x = self.drop(inputs)
        x = self.lin(x)
        return x

def add_linbndrop(input_layer, add_layers=[], activation=tf.keras.activations.relu, batchnorm=True, dropout_rate=None):
    if len(add_layers)>0:
        x=input_layer
        for i, units in enumerate(add_layers):
            x = LinBnDrop(units,
                          batchnorm,
                          dropout_rate,
                          activation,
                          'post_feature_embed'+str(units),
                          'post_feature_embed'+str(units)+'_'+str(i)+str(dropout_rate) )(x)
        return x
    else:
        return input_layer

from typing import Union
from keras import backend
def parse_args_initializer(initializer: Union[str, float, list]
                           ) -> tf.keras.initializers.Initializer:
    """Check process initializer input arguments"""
    if isinstance(initializer, str):
        return tf.keras.initializers.get(initializer)
    elif isinstance(initializer, (int, float, list)):
        return tf.keras.initializers.Constant(initializer)
    elif isinstance(initializer, tf.keras.initializers.Initializer):
        return initializer
    else:
        return None
class MultiLabelEmbedding(tf.keras.layers.Layer):
    """ Multi-label embedding for a fixed number of labels per data point
    Examples:
    ---------
    inputs = [[1, 2, 4], [0, 1, 2], [2, 1, 4], [3, 2, 1]]
    inputs = tf.constant(inputs)
    layer = MultiLabelEmbedding(
        vocab_size=500000, embed_size=300, random_state=42)
    y = layer(inputs)
    """
    def __init__(self,
                 vocab_size: int = None,
                 embed_size: int = None,
                 random_state: int = None,
                 initializer: Union[str, float, list] = 'uniform',
                 regularizer: str = None,
                 constraint: str = None,
                 mask_zero=False,
                 **kwargs):
        super(MultiLabelEmbedding, self).__init__(**kwargs)
        # store hyper params
        self.vocab_size = vocab_size   # v
        self.embed_size = embed_size   # e
        # other settings
        self.initializer = parse_args_initializer(initializer)
        self.regularizer = tf.keras.regularizers.get(regularizer)
        self.constraint = tf.keras.constraints.get(constraint)
        self.mask_zero = mask_zero
        # initialize layer weights
        if random_state:
            tf.random.set_seed(random_state)

    def build(self, input_shape=None):
        self.emb = self.add_weight(
            shape=(self.vocab_size, self.embed_size),
            initializer=self.initializer,
            regularizer=self.regularizer,
            constraint=self.constraint,
            trainable=self.trainable,
            name='multi_label_embedding')

    def call(self, inputs):
        dtype = backend.dtype(inputs)
        if dtype != 'int32' and dtype != 'int64':
            inputs = tf.cast(inputs, 'int32')
        h = tf.nn.embedding_lookup(params=self.emb, ids=inputs)
        h = tf.math.reduce_sum(h, axis=-2)
        # h = tf.math.reduce_mean(h, axis=-2)
        return h

    def compute_mask(self, inputs, mask=None):
        if not self.mask_zero:
            return None
        return tf.not_equal(inputs, 0)