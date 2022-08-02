from typing import List, Optional
import tensorflow as tf
from tensorflow.keras import backend as K
from tensorflow.keras.layers import Layer, Flatten, add, Concatenate, BatchNormalization, Activation, Dropout
from tensorflow.keras.initializers import glorot_normal, Zeros
from tensorflow.keras.regularizers import l2

from tensorflow.python.ops.lookup_ops import StaticHashTable
from tensorflow.python.ops.lookup_ops import TextFileInitializer

DEFAULT_MLP_LAYER_UNITS = (128, 128)  # [512, 256, 64]
SEED = 1024


# utils layers
class NoMask(Layer):
    def __init__(self, **kwargs):
        super(NoMask, self).__init__(**kwargs)

    def build(self, input_shape):
        # Be sure to call this somewhere!
        super(NoMask, self).build(input_shape)

    def call(self, x, mask=None, **kwargs):
        return x

    def compute_mask(self, inputs, mask):
        return None


class Linear(Layer):

    def __init__(self, l2_reg=0.0, mode=0, use_bias=False, seed=SEED, **kwargs):

        self.l2_reg = l2_reg
        if mode not in [0, 1, 2]:
            raise ValueError("mode must be 0,1 or 2")
        self.mode = mode
        self.use_bias = use_bias
        self.seed = seed
        super(Linear, self).__init__(**kwargs)

    def build(self, input_shape):
        if self.use_bias:
            self.bias = self.add_weight(name='linear_bias',
                                        shape=(1,),
                                        initializer=Zeros(),
                                        trainable=True)
        if self.mode == 1:
            self.kernel = self.add_weight(
                'linear_kernel',
                shape=[int(input_shape[-1]), 1],
                initializer=glorot_normal(self.seed),
                regularizer=l2(self.l2_reg),
                trainable=True)
        elif self.mode == 2:
            self.kernel = self.add_weight(
                'linear_kernel',
                shape=[int(input_shape[1][-1]), 1],
                initializer=glorot_normal(self.seed),
                regularizer=l2(self.l2_reg),
                trainable=True)

        super(Linear, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, inputs, **kwargs):
        if self.mode == 0:
            sparse_input = inputs
            linear_logit = tf.reduce_sum(sparse_input, axis=-1, keepdims=True)
        elif self.mode == 1:
            dense_input = inputs
            fc = tf.tensordot(dense_input, self.kernel, axes=(-1, 0))
            linear_logit = fc
        else:
            sparse_input, dense_input = inputs
            fc = tf.tensordot(dense_input, self.kernel, axes=(-1, 0))
            linear_logit = tf.reduce_sum(sparse_input, axis=-1, keepdims=False) + fc
        if self.use_bias:
            linear_logit += self.bias

        return linear_logit

    def compute_output_shape(self, input_shape):
        return (None, 1)

    def compute_mask(self, inputs, mask):
        return None

    def get_config(self, ):
        config = {'mode': self.mode, 'l2_reg': self.l2_reg, 'use_bias': self.use_bias, 'seed': self.seed}
        base_config = super(Linear, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


class CustomAdd(Layer):
    def __init__(self, **kwargs):
        super(CustomAdd, self).__init__(**kwargs)

    def build(self, input_shape):
        # Be sure to call this somewhere!
        super(CustomAdd, self).build(input_shape)

    def call(self, inputs, **kwargs):
        if not isinstance(inputs, list):
            return inputs
        if len(inputs) == 1:
            return inputs[0]
        if len(inputs) == 0:
            return tf.constant([[0.0]])

        return add(inputs)


def add_func(inputs):
    return CustomAdd()(inputs)


def combined_dnn_input(sparse_embedding_list, dense_value_list):
    if len(sparse_embedding_list) > 0 and len(dense_value_list) > 0:
        sparse_dnn_input = Flatten()(concat_func(sparse_embedding_list))
        dense_dnn_input = Flatten()(concat_func(dense_value_list))
        return concat_func([sparse_dnn_input, dense_dnn_input])
    elif len(sparse_embedding_list) > 0:
        return Flatten()(concat_func(sparse_embedding_list))
    elif len(dense_value_list) > 0:
        return Flatten()(concat_func(dense_value_list))
    else:
        raise NotImplementedError("dnn_feature_columns can not be empty list")


def concat_func(inputs, axis=-1, mask=False):
    if not mask:
        inputs = list(map(NoMask(), inputs))
    if len(inputs) == 1:
        return inputs[0]
    else:
        return Concatenate(axis=axis)(inputs)


############################################################################################
# activation layers

class Dice(Layer):
    """The Data Adaptive Activation Function in DIN,which can be viewed as a generalization of PReLu and can adaptively
    adjust the rectified point according to distribution of input data.

      Input shape
        - Arbitrary. Use the keyword argument `input_shape` (tuple of integers, does not include the samples' axis)
        when using this layer as the first layer in a model.

      Output shape
        - Same shape as the input.

      Arguments
        - **axis** : Integer, the axis that should be used to compute data distribution (typically the features axis).

        - **epsilon** : Small float added to variance to avoid dividing by zero.

      References
        - [Zhou G, Zhu X, Song C, et al. Deep interest network for click-through rate prediction[C]//
        Proceedings of the 24th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining.
        ACM, 2018: 1059-1068.](https://arxiv.org/pdf/1706.06978.pdf)
    """

    def __init__(self, axis=-1, epsilon=1e-9, **kwargs):
        self.axis = axis
        self.epsilon = epsilon
        super(Dice, self).__init__(**kwargs)

    def build(self, input_shape):
        self.bn = BatchNormalization(
            axis=self.axis, epsilon=self.epsilon, center=False, scale=False)
        self.alphas = self.add_weight(shape=(input_shape[-1],), initializer=Zeros(
        ), dtype=tf.float32, name='dice_alpha')  # name='alpha_'+self.name
        super(Dice, self).build(input_shape)  # Be sure to call this somewhere!
        self.uses_learning_phase = True

    def call(self, inputs, training=None, **kwargs):
        inputs_normed = self.bn(inputs, training=training)
        x_p = tf.sigmoid(inputs_normed)
        return self.alphas * (1.0 - x_p) * inputs + x_p * inputs

    def compute_output_shape(self, input_shape):
        return input_shape

    def get_config(self, ):
        config = {'axis': self.axis, 'epsilon': self.epsilon}
        base_config = super(Dice, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


def activation_layer(activation):
    if activation in ("dice", "Dice"):
        act_layer = Dice()
    elif isinstance(activation, str):
        act_layer = Activation(activation)
    elif issubclass(activation, Layer):
        act_layer = activation()
    else:
        raise ValueError(
            "Invalid activation,found %s.You should use a str or a Activation Layer Class." % (activation))
    return act_layer


############################################################################################
# core layers

class DNN(Layer):
    """The Multi Layer Perceptron

      Input shape - nD tensor with shape: ``(batch_size, ..., input_dim)``. The most common situation would be a 2D
      input with shape ``(batch_size, input_dim)``.

      Output shape - nD tensor with shape: ``(batch_size, ..., hidden_size[-1])``. For instance, for a 2D input with
      shape ``(batch_size, input_dim)``, the output would have shape ``(batch_size, hidden_size[-1])``.

      Arguments
        - **hidden_units**:list of positive integer, the layer number and units in each layer.

        - **activation**: Activation function to use.

        - **l2_reg**: float between 0 and 1. L2 regularizer strength applied to the kernel weights matrix.

        - **dropout_rate**: float in [0,1). Fraction of the units to dropout.

        - **use_bn**: bool. Whether use BatchNormalization before activation or not.

        - **output_activation**: Activation function to use in the last layer.If ``None``,it will be same as
        ``activation``.

        - **seed**: A Python integer to use as random seed.
    """

    def __init__(self, hidden_units, activation='relu', l2_reg=0, dropout_rate=0, use_bn=False, output_activation=None,
                 seed=SEED, **kwargs):
        self.hidden_units = hidden_units
        self.activation = activation
        self.l2_reg = l2_reg
        self.dropout_rate = dropout_rate
        self.use_bn = use_bn
        self.output_activation = output_activation
        self.seed = seed

        super(DNN, self).__init__(**kwargs)

    def build(self, input_shape):
        # if len(self.hidden_units) == 0:
        #     raise ValueError("hidden_units is empty")
        input_size = input_shape[-1]
        hidden_units = [int(input_size)] + list(self.hidden_units)
        self.kernels = [self.add_weight(name='kernel' + str(i),
                                        shape=(
                                            hidden_units[i], hidden_units[i + 1]),
                                        initializer=glorot_normal(
                                            seed=self.seed),
                                        regularizer=l2(self.l2_reg),
                                        trainable=True) for i in range(len(self.hidden_units))]
        self.bias = [self.add_weight(name='bias' + str(i),
                                     shape=(self.hidden_units[i],),
                                     initializer=Zeros(),
                                     trainable=True) for i in range(len(self.hidden_units))]
        if self.use_bn:
            self.bn_layers = [BatchNormalization() for _ in range(len(self.hidden_units))]

        self.dropout_layers = [Dropout(self.dropout_rate, seed=self.seed + i) for i in
                               range(len(self.hidden_units))]

        self.activation_layers = [activation_layer(self.activation) for _ in range(len(self.hidden_units))]

        if self.output_activation:
            self.activation_layers[-1] = activation_layer(self.output_activation)

        super(DNN, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, inputs, training=None, **kwargs):

        deep_input = inputs

        for i in range(len(self.hidden_units)):
            fc = tf.nn.bias_add(tf.tensordot(
                deep_input, self.kernels[i], axes=(-1, 0)), self.bias[i])

            if self.use_bn:
                fc = self.bn_layers[i](fc, training=training)
            try:
                fc = self.activation_layers[i](fc, training=training)
            except TypeError as e:  # TypeError: call() got an unexpected keyword argument 'training'
                print("make sure the activation function use training flag properly", e)
                fc = self.activation_layers[i](fc)

            fc = self.dropout_layers[i](fc, training=training)
            deep_input = fc

        return deep_input

    def compute_output_shape(self, input_shape):
        if len(self.hidden_units) > 0:
            shape = input_shape[:-1] + (self.hidden_units[-1],)
        else:
            shape = input_shape

        return tuple(shape)

    def get_config(self, ):
        config = {'activation': self.activation, 'hidden_units': self.hidden_units,
                  'l2_reg': self.l2_reg, 'use_bn': self.use_bn, 'dropout_rate': self.dropout_rate,
                  'output_activation': self.output_activation, 'seed': self.seed}
        base_config = super(DNN, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


class PredictionLayer(Layer):
    """
      Arguments
         - **task**: str, ``"binary"`` for  binary logloss or  ``"regression"`` for regression loss

         - **use_bias**: bool.Whether add bias term or not.
    """

    def __init__(self, task='binary', use_bias=True, **kwargs):
        if task not in ["binary", "multiclass", "regression"]:
            raise ValueError("task must be binary,multiclass or regression")
        self.task = task
        self.use_bias = use_bias
        super(PredictionLayer, self).__init__(**kwargs)

    def build(self, input_shape):

        if self.use_bias:
            self.global_bias = self.add_weight(
                shape=(1,), initializer=Zeros(), name="global_bias")

        # Be sure to call this somewhere!
        super(PredictionLayer, self).build(input_shape)

    def call(self, inputs, **kwargs):
        x = inputs
        if self.use_bias:
            x = tf.nn.bias_add(x, self.global_bias, data_format='NHWC')
        if self.task == "binary":
            x = tf.sigmoid(x)

        output = tf.reshape(x, (-1, 1))

        return output

    def compute_output_shape(self, input_shape):
        return (None, 1)

    def get_config(self, ):
        config = {'task': self.task, 'use_bias': self.use_bias}
        base_config = super(PredictionLayer, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


############################################################################################
# interaction layers

class FM(Layer):
    """Factorization Machine models pairwise (order-2) feature interactions
     without linear term and bias.

      Input shape
        - 3D tensor with shape: ``(batch_size,field_size,embedding_size)``.

      Output shape
        - 2D tensor with shape: ``(batch_size, 1)``.

      References
        - [Factorization Machines](https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf)
    """

    def __init__(self, **kwargs):

        super(FM, self).__init__(**kwargs)

    def build(self, input_shape):
        if len(input_shape) != 3:
            raise ValueError("Unexpected inputs dimensions % d,\
                             expect to be 3 dimensions" % (len(input_shape)))

        super(FM, self).build(input_shape)  # Be sure to call this somewhere!

    def call(self, inputs, **kwargs):

        if K.ndim(inputs) != 3:
            raise ValueError(
                "Unexpected inputs dimensions %d, expect to be 3 dimensions"
                % (K.ndim(inputs)))

        concated_embeds_value = inputs

        square_of_sum = tf.square(tf.reduce_sum(
            concated_embeds_value, axis=1, keepdims=True))
        sum_of_square = tf.reduce_sum(
            concated_embeds_value * concated_embeds_value, axis=1, keepdims=True)
        cross_term = square_of_sum - sum_of_square
        cross_term = 0.5 * tf.reduce_sum(cross_term, axis=2, keepdims=False)

        return cross_term

    def compute_output_shape(self, input_shape):
        return (None, 1)


# class InnerProductLayer(Layer):
#     """InnerProduct Layer used in PNN that compute the element-wise
#     product or inner product between feature vectors.
#
#       Input shape
#         - a list of 3D tensor with shape: ``(batch_size,1,embedding_size)``.
#
#       Output shape
#         - 3D tensor with shape: ``(batch_size, N*(N-1)/2 ,1)`` if use reduce_sum. or 3D tensor with shape: ``(batch_size, N*(N-1)/2, embedding_size )`` if not use reduce_sum.
#
#       Arguments
#         - **reduce_sum**: bool. Whether return inner product or element-wise product
#
#       References
#             - [Qu Y, Cai H, Ren K, et al. Product-based neural networks for user response prediction[C]//Data Mining (ICDM), 2016 IEEE 16th International Conference on. IEEE, 2016: 1149-1154.](https://arxiv.org/pdf/1611.00144.pdf)
#     """
#
#     def __init__(self, reduce_sum=True, **kwargs):
#         self.reduce_sum = reduce_sum
#         super(InnerProductLayer, self).__init__(**kwargs)
#
#     def build(self, input_shape):
#
#         if not isinstance(input_shape, list) or len(input_shape) < 2:
#             raise ValueError('A `InnerProductLayer` layer should be called '
#                              'on a list of at least 2 inputs')
#
#         reduced_inputs_shapes = [shape.as_list() for shape in input_shape]
#         shape_set = set()
#
#         for i in range(len(input_shape)):
#             shape_set.add(tuple(reduced_inputs_shapes[i]))
#
#         if len(shape_set) > 1:
#             raise ValueError('A `InnerProductLayer` layer requires '
#                              'inputs with same shapes '
#                              'Got different shapes: %s' % (shape_set))
#
#         if len(input_shape[0]) != 3 or input_shape[0][1] != 1:
#             raise ValueError('A `InnerProductLayer` layer requires '
#                              'inputs of a list with same shape tensor like (None,1,embedding_size)'
#                              'Got different shapes: %s' % (input_shape[0]))
#         super(InnerProductLayer, self).build(
#             input_shape)  # Be sure to call this somewhere!
#
#     def call(self, inputs, **kwargs):
#         if K.ndim(inputs[0]) != 3:
#             raise ValueError(
#                 "Unexpected inputs dimensions %d, expect to be 3 dimensions" % (K.ndim(inputs)))
#
#         embed_list = inputs
#         row = []
#         col = []
#         num_inputs = len(embed_list)
#
#         for i in range(num_inputs - 1):
#             for j in range(i + 1, num_inputs):
#                 row.append(i)
#                 col.append(j)
#         p = tf.concat([embed_list[idx]
#                        for idx in row], axis=1)  # batch num_pairs k
#         q = tf.concat([embed_list[idx]
#                        for idx in col], axis=1)
#
#         inner_product = p * q
#         if self.reduce_sum:
#             inner_product = tf.reduce_sum(
#                 inner_product, axis=2, keepdims=True)
#         return inner_product
#
#     def compute_output_shape(self, input_shape):
#         num_inputs = len(input_shape)
#         num_pairs = int(num_inputs * (num_inputs - 1) / 2)
#         input_shape = input_shape[0]
#         embed_size = input_shape[-1]
#         if self.reduce_sum:
#             return (input_shape[0], num_pairs, 1)
#         else:
#             return (input_shape[0], num_pairs, embed_size)
#
#     def get_config(self, ):
#         config = {'reduce_sum': self.reduce_sum, }
#         base_config = super(InnerProductLayer, self).get_config()
#         base_config.update(config)
#         return base_config


############################################################################################
# Not used in the current DeepFM model, but will be useful if we use contexual information
class WeightedSequenceLayer(Layer):
    """The WeightedSequenceLayer is used to apply weight score on variable-length sequence feature/multi-value feature.

      Input shape
        - A list of three  tensor [seq_value,seq_len,seq_weight]

        - seq_value is a 3D tensor with shape: ``(batch_size, T, embedding_size)``

        - seq_len is a 2D tensor with shape : ``(batch_size, 1)``,indicate valid length of each sequence.

        - seq_weight is a 3D tensor with shape: ``(batch_size, T, 1)``

      Output shape
        - 3D tensor with shape: ``(batch_size, T, embedding_size)``.

      Arguments
        - **weight_normalization**: bool.Whether normalize the weight score before applying to sequence.

        - **supports_masking**:If True,the input need to support masking.
    """

    def __init__(self, weight_normalization=True, supports_masking=False, **kwargs):
        super(WeightedSequenceLayer, self).__init__(**kwargs)
        self.weight_normalization = weight_normalization
        self.supports_masking = supports_masking

    def build(self, input_shape):
        if not self.supports_masking:
            self.seq_len_max = int(input_shape[0][1])
        super(WeightedSequenceLayer, self).build(
            input_shape)  # Be sure to call this somewhere!

    def call(self, input_list, mask=None, **kwargs):
        if self.supports_masking:
            if mask is None:
                raise ValueError(
                    "When supports_masking=True,input must support masking")
            key_input, value_input = input_list
            mask = tf.expand_dims(mask[0], axis=2)
        else:
            key_input, key_length_input, value_input = input_list
            mask = tf.sequence_mask(key_length_input,
                                    self.seq_len_max, dtype=tf.bool)
            mask = tf.transpose(mask, (0, 2, 1))

        embedding_size = key_input.shape[-1]

        if self.weight_normalization:
            paddings = tf.ones_like(value_input) * (-2 ** 32 + 1)
        else:
            paddings = tf.zeros_like(value_input)
        value_input = tf.where(mask, value_input, paddings)

        if self.weight_normalization:
            value_input = tf.nn.softmax(value_input, axis=1)

        if len(value_input.shape) == 2:
            value_input = tf.expand_dims(value_input, axis=2)
            value_input = tf.tile(value_input, [1, 1, embedding_size])

        return tf.multiply(key_input, value_input)

    def compute_output_shape(self, input_shape):
        return input_shape[0]

    def compute_mask(self, inputs, mask):
        if self.supports_masking:
            return mask[0]
        else:
            return None

    def get_config(self, ):
        config = {'weight_normalization': self.weight_normalization, 'supports_masking': self.supports_masking}
        base_config = super(WeightedSequenceLayer, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


class SequencePoolingLayer(Layer):
    """The SequencePoolingLayer is used to apply pooling operation(sum,mean,max) on variable-length sequence feature/multi-value feature.

      Input shape
        - A list of two  tensor [seq_value,seq_len]

        - seq_value is a 3D tensor with shape: ``(batch_size, T, embedding_size)``

        - seq_len is a 2D tensor with shape : ``(batch_size, 1)``,indicate valid length of each sequence.

      Output shape
        - 3D tensor with shape: ``(batch_size, 1, embedding_size)``.

      Arguments
        - **mode**:str.Pooling operation to be used,can be sum,mean or max.

        - **supports_masking**:If True,the input need to support masking.
    """

    def __init__(self, mode='mean', supports_masking=False, **kwargs):

        if mode not in ['sum', 'mean', 'max']:
            raise ValueError("mode must be sum or mean")
        self.mode = mode
        self.eps = tf.constant(1e-8, tf.float32)
        super(SequencePoolingLayer, self).__init__(**kwargs)

        self.supports_masking = supports_masking

    def build(self, input_shape):
        if not self.supports_masking:
            self.seq_len_max = int(input_shape[0][1])
        super(SequencePoolingLayer, self).build(
            input_shape)  # Be sure to call this somewhere!

    def call(self, seq_value_len_list, mask=None, **kwargs):
        if self.supports_masking:
            if mask is None:
                raise ValueError(
                    "When supports_masking=True,input must support masking")
            uiseq_embed_list = seq_value_len_list
            mask = tf.cast(mask, tf.float32)  # tf.to_float(mask)
            user_behavior_length = tf.reduce_sum(mask, axis=-1, keepdims=True)
            mask = tf.expand_dims(mask, axis=2)
        else:
            uiseq_embed_list, user_behavior_length = seq_value_len_list

            mask = tf.sequence_mask(user_behavior_length,
                                    self.seq_len_max, dtype=tf.float32)
            mask = tf.transpose(mask, (0, 2, 1))

        embedding_size = uiseq_embed_list.shape[-1]

        mask = tf.tile(mask, [1, 1, embedding_size])

        if self.mode == "max":
            hist = uiseq_embed_list - (1 - mask) * 1e9
            return tf.reduce_max(hist, 1, keepdims=True)

        hist = tf.reduce_sum(uiseq_embed_list * mask, 1, keepdims=False)

        if self.mode == "mean":
            hist = tf.divide(hist, tf.cast(user_behavior_length, tf.float32) + self.eps)

        hist = tf.expand_dims(hist, axis=1)
        return hist

    def compute_output_shape(self, input_shape):
        if self.supports_masking:
            return (None, 1, input_shape[-1])
        else:
            return (None, 1, input_shape[0][-1])

    def compute_mask(self, inputs, mask):
        return None

    def get_config(self, ):
        config = {'mode': self.mode, 'supports_masking': self.supports_masking}
        base_config = super(SequencePoolingLayer, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


class Hash(Layer):
    """Looks up keys in a table when setup `vocabulary_path`, which outputs the corresponding values.
    If `vocabulary_path` is not set, `Hash` will hash the input to [0,num_buckets). When `mask_zero` = True,
    input value `0` or `0.0` will be set to `0`, and other value will be set in range [1,num_buckets).

    The following snippet initializes a `Hash` with `vocabulary_path` file with the first column as keys and
    second column as values:

    * `1,emerson`
    * `2,lake`
    * `3,palmer`

    >>> hash = Hash(
    ...   num_buckets=3+1,
    ...   vocabulary_path=filename,
    ...   default_value=0)
    >>> hash(tf.constant('lake')).numpy()
    2
    >>> hash(tf.constant('lakeemerson')).numpy()
    0

    Args:
        num_buckets: An `int` that is >= 1. The number of buckets or the vocabulary size + 1
            when `vocabulary_path` is setup.
        mask_zero: default is False. The `Hash` value will hash input `0` or `0.0` to value `0` when
            the `mask_zero` is `True`. `mask_zero` is not used when `vocabulary_path` is setup.
        vocabulary_path: default `None`. The `CSV` text file path of the vocabulary hash, which contains
            two columns seperated by delimiter `comma`, the first column is the value and the second is
            the key. The key data type is `string`, the value data type is `int`. The path must
            be accessible from wherever `Hash` is initialized.
        default_value: default '0'. The default value if a key is missing in the table.
        **kwargs: Additional keyword arguments.
    """

    def __init__(self, num_buckets, mask_zero=False, vocabulary_path=None, default_value=0, **kwargs):
        self.num_buckets = num_buckets
        self.mask_zero = mask_zero
        self.vocabulary_path = vocabulary_path
        self.default_value = default_value
        if self.vocabulary_path:
            initializer = TextFileInitializer(vocabulary_path, 'string', 1, 'int64', 0, delimiter=',')
            self.hash_table = StaticHashTable(initializer, default_value=self.default_value)
        super(Hash, self).__init__(**kwargs)

    def build(self, input_shape):
        # Be sure to call this somewhere!
        super(Hash, self).build(input_shape)

    def call(self, x, mask=None, **kwargs):

        if x.dtype != tf.string:
            zero = tf.as_string(tf.zeros([1], dtype=x.dtype))
            x = tf.as_string(x, )
        else:
            zero = tf.as_string(tf.zeros([1], dtype='int32'))

        if self.vocabulary_path:
            hash_x = self.hash_table.lookup(x)
            return hash_x

        num_buckets = self.num_buckets if not self.mask_zero else self.num_buckets - 1
        try:
            hash_x = tf.string_to_hash_bucket_fast(x, num_buckets,
                                                   name=None)  # weak hash
        except:
            hash_x = tf.strings.to_hash_bucket_fast(x, num_buckets,
                                                    name=None)  # weak hash
        if self.mask_zero:
            mask = tf.cast(tf.not_equal(x, zero), dtype='int64')
            hash_x = (hash_x + 1) * mask

        return hash_x

    def compute_output_shape(self, input_shape):
        return input_shape

    def get_config(self, ):
        config = {'num_buckets': self.num_buckets, 'mask_zero': self.mask_zero, 'vocabulary_path': self.vocabulary_path,
                  'default_value': self.default_value}
        base_config = super(Hash, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))


custom_objects = {'tf': tf,
                  # 'InnerProductLayer': InnerProductLayer,
                  'DNN': DNN,
                  'PredictionLayer': PredictionLayer,
                  'FM': FM,
                  'Dice': Dice,
                  'SequencePoolingLayer': SequencePoolingLayer,
                  'NoMask': NoMask,
                  'Hash': Hash,
                  'Linear': Linear,
                  'WeightedSequenceLayer': WeightedSequenceLayer,
                  'CustomAdd': CustomAdd
                  }
