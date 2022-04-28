import tensorflow as tf


class VocabLookup(tf.keras.layers.Layer):
    def __init__(self, vocab_path):
        super(VocabLookup, self).__init__(trainable=False, dtype=tf.string)
        self.vocab_path = vocab_path

    def build(self, input_shape):
        table_init = tf.lookup.TextFileInitializer( filename = self.vocab_path,
                                                    key_dtype = tf.string,
                                                    key_index = 0,
                                                    value_dtype = tf.string,
                                                    value_index = 1,
                                                    delimiter = ",")
        self.table = tf.lookup.StaticHashTable(table_init, 'UNK')
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