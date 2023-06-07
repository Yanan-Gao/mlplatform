from tensorflow import keras
import tensorflow as tf
from tensorflow.keras import layers, models
from tensorflow.python.keras.engine import data_adapter
import functools
import ctypes
import os


def search_layer(model, name, exclude):
    """
    Given the compiled model, find the layers with the key word name
    """
    layers = []
    for layer in model.layers:
        if name in layer.name:
            layers.append(layer)
    if not exclude and not layers:
        layers = [layer for layer in layers if layer.name not in exclude]
    # currently, we only support one layer, thus, here we choose the first one
    return layers[0]


def FGM(model, name, epsilon=0.5, exclude=None):
    target_layer = search_layer(model, name, exclude)
    if not target_layer:
        raise ValueError("layer_name has to be valid name for the model layers")

    # fast gradient method: adversarial training method
    @tf.function
    def train_step(self, data):
        """
        When we want to use adversarial training, use the following code on the complied model, for nlp we use it to add the permutation on embedding layers
        for cv, we add the permutation to layer we want in general after the input layers
        1. calculate the permutation on the embedding
        2. add the permutation on the embedding
        3. calculate the gradient, loss, update weights and remove the permutation
        """
        data = data_adapter.expand_1d(data)
        x, y, sample_weight = data_adapter.unpack_x_y_sample_weight(data)
        # caculate the permutation on the target layers and add it to the layer
        with tf.GradientTape() as tape:
            y_pred = self(x, training=True)
            loss = self.compiled_loss(
                y, y_pred, sample_weight, regularization_losses=self.losses
            )

        target_layer_weights = target_layer.weights
        target_layer_weights_gradients = tape.gradient(loss, target_layer_weights)[0]
        target_layer_weights_gradients = tf.squeeze(
            tf.zeros_like(target_layer_weights) + target_layer_weights_gradients
        )
        # calculate permutation
        delta = (
            epsilon * target_layer_weights_gradients / (tf.math.sqrt(tf.reduce_sum(target_layer_weights_gradients ** 2)) + 1e-8)
        )

        target_layer_weights[0].assign_add(delta)

        # calculate the loss based on the layer with permutation and remove the permutation before updating the weights
        with tf.GradientTape() as tape2:
            y_pred = self(x, training=True)
            new_loss = self.compiled_loss(
                y, y_pred, sample_weight, regularization_losses=self.losses
            )
        grads = tape2.gradient(new_loss, self.trainable_variables)
        target_layer_weights[0].assign_sub(delta)

        self.optimizer.apply_gradients(zip(grads, self.trainable_variables))
        self.compiled_metrics.update_state(y, y_pred, sample_weight)

        return {m.name: m.result() for m in self.metrics}

    return train_step


def FGM_wrapper(model, name, epsilon=0.5, exclude=None):
    """
    if the model has so many dropout or the dropout ratio is large, then the epsilon should be small
    otherwise, the adversarial training will affect the stability of the gradient descent and worse the model performance
    """
    train_step = FGM(model, name, epsilon, exclude)
    model.train_step = functools.partial(train_step, model)
    model.train_function = None


def GPU_setting():
    # Reduce gpu fragmentation
    os.environ["TF_GPU_ALLOCATOR"] = "cuda_malloc_async"
    gpus = tf.config.experimental.list_physical_devices("GPU")
    for gpu in gpus:
        tf.config.experimental.set_memory_growth(gpu, True)

    # L2 cache setting
    _libcudart = ctypes.CDLL("libcudart.so")
    # Set device limit on the current device
    # cudaLimitMaxL2FetchGranularity = 0x05
    pValue = ctypes.cast((ctypes.c_int * 1)(), ctypes.POINTER(ctypes.c_int))
    _libcudart.cudaDeviceSetLimit(ctypes.c_int(0x05), ctypes.c_int(128))
    _libcudart.cudaDeviceGetLimit(pValue, ctypes.c_int(0x05))
    assert pValue.contents.value == 128


class poly1_cross_entropy(tf.keras.losses.Loss):
    def __init__(self, label_smoothing=0, epsilon=1):
        super().__init__()
        self.label_smoothing = label_smoothing
        self.epsilon = epsilon

    def call(self, y_true, y_pred):
        ce_loss = tf.keras.losses.BinaryCrossentropy(
            from_logits=False,
            label_smoothing=self.label_smoothing,
        )(y_true, y_pred)
        # add label smoothing to poly
        y_true1 = y_true * (1 - self.label_smoothing) + self.label_smoothing / 2
        one_minus_pt = tf.reduce_sum(y_true1 * (1 - y_pred), axis=-1)
        poly1_loss = ce_loss + self.epsilon * one_minus_pt
        return poly1_loss


def value_feature(name, dtype=tf.float32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    return i, i


def get_initialiser(initializer="he_normal", seed=13):
    if initializer == "he_normal":
        return tf.keras.initializers.HeNormal(seed)
    else:
        print('Currently not support other initializer. Use GlorotNormal instead')
        return tf.keras.initializers.GlorotNormal(seed)


def embedding(
        name, vocab_size=10000, emb_dim=40, dtype=tf.int32, dropout_rate=0.2, seed=13, initializer='he_normal'
):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    em = layers.Embedding(
        input_dim=vocab_size,
        output_dim=emb_dim,
        name=f"embedding_{name}",
        embeddings_initializer=get_initialiser(initializer, seed=seed),
        # mask_zero=True,
    )  # output shape: (None,1,emb_dim)
    f = layers.Flatten(name=f"flatten_{name}")  # flatten output shape: (None,1*emb_dim)
    dr = layers.Dropout(seed=seed, rate=dropout_rate, name=f"layer_{name}_dropout")
    return i, dr(f(em(i)))


def list_to_embedding(name, vocab_size, em_size, dropout_rate=0.2, seed=13, initializer='he_normal'):
    i = keras.Input(shape=(1, None), dtype=tf.int32, name=f"{name}")
    em = layers.Embedding(
        input_dim=vocab_size,
        output_dim=em_size,
        name=f"embedding_{name}",
        embeddings_initializer=get_initialiser(initializer, seed=seed),
        # mask_zero=True,
    )
    # use for the vary length matrix
    re = layers.Reshape(target_shape=(-1, em_size), name=f"reshape_{name}")
    dr = layers.Dropout(seed=seed, rate=dropout_rate, name=f"layer_{name}_dropout")
    return i, dr(re(em(i)))


class TransformBlock(tf.keras.Model):
    def __init__(
            self, features, momentum=0.9, virtual_batch_size=None, block_name="", **kwargs
    ):
        super(TransformBlock, self).__init__(**kwargs)

        self.features = features
        self.momentum = momentum
        self.virtual_batch_size = virtual_batch_size

        self.transform = layers.Dense(
            self.features, use_bias=False, name=f"transformblock_dense_{block_name}"
        )

        # in case that the virtual_batch_size cannot work
        self.bn = tf.keras.layers.BatchNormalization(
            axis=-1,
            momentum=momentum,
            virtual_batch_size=virtual_batch_size,
            name=f"transformblock_bn_{block_name}",
        )

    def call(self, inputs, training=True):
        x = self.transform(inputs)
        x = self.bn(x, training=training)
        return x


class TabNet(tf.keras.Model):
    def __init__(
            self,
            num_features,
            feature_dim=64,
            output_dim=64,
            num_decision_steps=5,
            relaxation_factor=1.5,
            batch_momentum=0.98,
            virtual_batch_size=None,
            epsilon=1e-5,
            **kwargs,
    ):
        """
        a few general principles on hyperparameter
        selection:
            - Most datasets yield the best results for Nsteps ∈ [3, 10]. Typically, larger datasets and
            more complex tasks require a larger Nsteps. A very high value of Nsteps may suffer from
            overfitting and yield poor generalization.
            - Adjustment of the values of Nd and Na is the most efficient way of obtaining a trade-off
            between performance and complexity. Nd = Na is a reasonable choice for most datasets. A
            very high value of Nd and Na may suffer from overfitting and yield poor generalization.
            - A large batch size is beneficial for performance - if the memory constraints permit, as large
            as 1-10 % of the total training dataset size is suggested. The virtual batch size is typically
            much smaller than the batch size.
            - Initially large learning rate is important, which should be gradually decayed until convergence.
        Args:
            feature_dim (N_a): Dimensionality of the hidden representation in feature
                transformation block. Each layer first maps the representation to a
                2*feature_dim-dimensional output and half of it is used to determine the
                nonlinearity of the GLU activation where the other half is used as an
                input to GLU, and eventually feature_dim-dimensional output is
                transferred to the next layer.
            output_dim (N_d): Dimensionality of the outputs of each decision step, which is
                later mapped to the final classification or regression output.
            num_features: The number of input features (i.e the number of columns for
                tabular data assuming each feature is represented with 1 dimension).
            num_decision_steps(N_steps): Number of sequential decision steps.
            relaxation_factor (gamma): Relaxation factor that promotes the reuse of each
                feature at different decision steps. When it is 1, a feature is enforced
                to be used only at one decision step and as it increases, more
                flexibility is provided to use a feature at multiple decision steps.
            batch_momentum: Momentum in ghost batch normalization.
            virtual_batch_size: Virtual batch size in ghost batch normalization. The
                overall batch size should be an integer multiple of virtual_batch_size.
            epsilon: A small number for numerical stability of the entropy calculations.
        """
        super(TabNet, self).__init__(**kwargs)

        # Input checks
        if num_decision_steps < 1:
            raise ValueError("Num decision steps must be greater than 0.")

        if feature_dim <= output_dim:
            raise ValueError(
                "To compute `features_for_coef`, feature_dim must be larger than output dim"
            )

        feature_dim = int(feature_dim)
        output_dim = int(output_dim)
        num_decision_steps = int(num_decision_steps)
        relaxation_factor = float(relaxation_factor)
        batch_momentum = float(batch_momentum)
        epsilon = float(epsilon)

        if relaxation_factor < 0.0:
            raise ValueError("`relaxation_factor` cannot be negative !")

        if virtual_batch_size is not None:
            virtual_batch_size = int(virtual_batch_size)

        self.num_features = num_features
        self.feature_dim = feature_dim
        self.output_dim = output_dim

        self.num_decision_steps = num_decision_steps
        self.relaxation_factor = relaxation_factor
        self.batch_momentum = batch_momentum
        self.virtual_batch_size = virtual_batch_size
        self.epsilon = epsilon

        self.input_bn = tf.keras.layers.BatchNormalization(
            axis=-1, momentum=batch_momentum, name="input_bn"
        )

        self.transform_f1 = TransformBlock(
            self.feature_dim,
            self.batch_momentum,
            self.virtual_batch_size,
            block_name="f1",
        )

        self.transform_f2 = TransformBlock(
            self.feature_dim,
            self.batch_momentum,
            self.virtual_batch_size,
            block_name="f2",
        )

        self.transform_f3_list = [
            TransformBlock(
                self.feature_dim,
                self.batch_momentum,
                self.virtual_batch_size,
                block_name=f"f3_{i}",
            )
            for i in range(self.num_decision_steps)
        ]

        self.transform_f4_list = [
            TransformBlock(
                self.feature_dim,
                self.batch_momentum,
                self.virtual_batch_size,
                block_name=f"f4_{i}",
            )
            for i in range(self.num_decision_steps)
        ]

        self.transform_coef_list = [
            TransformBlock(
                self.num_features,
                self.batch_momentum,
                self.virtual_batch_size,
                block_name=f"coef_{i}",
            )
            for i in range(self.num_decision_steps - 1)
        ]

        self._step_feature_selection_masks = None
        self._step_aggregate_feature_selection_mask = None

    def call(self, inputs, training=True):
        features = self.input_bn(inputs, training=training)

        batch_size = tf.shape(features)[0]
        self._step_feature_selection_masks = []
        self._step_aggregate_feature_selection_mask = None

        # Initializes decision-step dependent variables.
        # aggregate for the final output
        output_aggregated = tf.zeros(
            [batch_size, self.output_dim], dtype=self.compute_dtype
        )
        # even the beginning, the features can be considered as masked one without using mask
        masked_features = features
        # the mask for the input features
        mask_values = tf.zeros(
            [batch_size, self.num_features], dtype=self.compute_dtype
        )
        aggregated_mask_values = tf.zeros(
            [batch_size, self.num_features], dtype=self.compute_dtype
        )
        complementary_aggregated_mask_values = tf.ones(
            [batch_size, self.num_features], dtype=self.compute_dtype
        )

        for ni in range(self.num_decision_steps):
            # Feature transformer with two shared and two decision step dependent
            # blocks is used below.=
            transform_f1 = self.transform_f1(masked_features, training=training)

            transform_f2 = self.transform_f2(transform_f1, training=training)

            transform_f2 = transform_f2 * tf.constant(
                0.4, dtype=self.compute_dtype
            ) + transform_f1 * tf.constant(0.8, dtype=self.compute_dtype)

            transform_f3 = self.transform_f3_list[ni](transform_f2, training=training)

            transform_f3 = transform_f3 * tf.constant(
                0.4, dtype=self.compute_dtype
            ) + transform_f2 * tf.constant(0.8, dtype=self.compute_dtype)

            transform_f4 = self.transform_f4_list[ni](transform_f3, training=training)

            transform_f4 = transform_f4 * tf.constant(
                0.4, dtype=self.compute_dtype
            ) + transform_f3 * tf.constant(0.8, dtype=self.compute_dtype)

            if ni > 0 or self.num_decision_steps == 1:
                decision_out = tf.nn.relu(transform_f4[:, : self.output_dim])

                # Decision aggregation.
                output_aggregated += decision_out

                # Aggregated masks are used for visualization of the
                # feature importance attributes.
                scale_agg = tf.reduce_sum(decision_out, axis=1, keepdims=True)

                if self.num_decision_steps > 1:
                    scale_agg = scale_agg / tf.cast(
                        self.num_decision_steps - 1, self.compute_dtype
                    )

                aggregated_mask_values += mask_values * scale_agg

            features_for_coef = transform_f4[:, self.output_dim:]

            if ni < (self.num_decision_steps - 1):
                # Determines the feature masks via linear and nonlinear
                # transformations, taking into account of aggregated feature use.
                mask_values = self.transform_coef_list[ni](
                    features_for_coef, training=training
                )
                mask_values *= complementary_aggregated_mask_values

                mask_values = tf.nn.softmax(
                    mask_values * tf.constant(100.0, dtype=self.compute_dtype), axis=-1
                )

                # Relaxation factor controls the amount of reuse of features between
                # different decision blocks and updated with the values of
                # coefficients.
                complementary_aggregated_mask_values *= (
                    self.relaxation_factor - mask_values
                )

                # Feature selection.
                masked_features = tf.multiply(mask_values, features)

                mask_at_step_i = tf.expand_dims(tf.expand_dims(mask_values, 0), 3)
                self._step_feature_selection_masks.append(mask_at_step_i)

        agg_mask = tf.expand_dims(tf.expand_dims(aggregated_mask_values, 0), 3)
        self._step_aggregate_feature_selection_mask = agg_mask

        return output_aggregated

    @property
    def feature_selection_masks(self):
        return self._step_feature_selection_masks

    @property
    def aggregate_feature_selection_mask(self):
        return self._step_aggregate_feature_selection_mask


def init_model(
        model_features,
        model_dim_group,
        neo_emb_size,
        feature_dim_factor,
        num_decision_steps,
        relaxation_factor=1.5,
        tabnet_factor=0.15,
        embedding_factor=1,
        dropout_rate=0.2,
        seed=13,
        sum_residual_dropout=False,
        sum_residual_dropout_rate=0.4,
        ignore_index=None,
        model_name="Audience_Extension",
        initializer="he_normal",
):
    model_input_features_tuple = [
        embedding(
            name=f.name,
            vocab_size=f.cardinality,
            emb_dim=f.embedding_dim,
            dtype=f.type,
            seed=seed,
            initializer=initializer,
        )
        if f.type == tf.int32
        else value_feature(f.name)
        for f in model_features
    ]
    model_input_features = [x[0] for x in model_input_features_tuple]
    #     model_input_layers = [x[1] for x in model_input_features_tuple]
    if not ignore_index:
        ignore_index = [ignore_index]

    model_input_layers = [
        x[1] for x in model_input_features_tuple if x[0].name not in ignore_index
    ]
    model_input_layers = layers.concatenate(
        model_input_layers, name="bidimpression_concat_embedding"
    )
    # to make the output of the bidimpression tensor as long as the neo embedding size
    model_input_layers = layers.Dense(
        neo_emb_size,
        kernel_initializer=tf.keras.initializers.HeNormal(seed=seed),
        name='bidimpression_neo_embedding',
    )(model_input_layers)

    # model_input_dim = list_to_embedding(model_dim_group[0].name, model_dim_group[0].cardinality, search_emb_size) # model_input_layers.shape[1])
    model_input_dim = list_to_embedding(
        name=model_dim_group[0].name,
        vocab_size=model_dim_group[0].cardinality,
        em_size=model_input_layers.shape[1],
        dropout_rate=dropout_rate,
        seed=seed,
        initializer=initializer,
    )
    model_inputs_dim = model_input_dim[0]
    input_layer_dim = model_input_dim[1]

    model_input = model_input_features + [model_inputs_dim]

    # add one more dense here to make sure the impression site and pixel site's embedding size are correct
    # model_input_layers = layers.Dense(search_emb_size, activation=None, kernel_initializer=tf.keras.initializers.HeNormal(), name=f"embedding{search_emb_size}_imp")(model_input_layers)

    #     multiply_lambda = lambda array: tf.keras.layers.multiply([array[0], array[1]])
    #     residual1 = layers.Lambda(multiply_lambda)([model_input_layers, input_layer_dim])
    # residual1 = tf.math.multiply(tf.expand_dims(model_input_layers,1),input_layer_dim)

    tabnet = TabNet(
        num_features=model_input_layers.shape[-1],
        feature_dim=int(feature_dim_factor * input_layer_dim.shape[-1]),
        output_dim=input_layer_dim.shape[-1],
        num_decision_steps=num_decision_steps,
        relaxation_factor=relaxation_factor,
    )
    tab = tabnet(model_input_layers)

    # residual2 = layers.Lambda(multiply_lambda)([tab, input_layer_dim])
    # residual2 = tf.math.multiply(tf.expand_dims(tab,1), input_layer_dim)
    # output = residual2 * tf.constant(tabnet_factor) + residual1 * embedding_factor
    bidimp = layers.Add(name='bidimpression_result')([tf.expand_dims(tab, 1) * tabnet_factor, tf.expand_dims(model_input_layers, 1) * embedding_factor])
    output = tf.math.multiply(bidimp, input_layer_dim)

    if sum_residual_dropout:
        dr = layers.Dropout(
            seed=seed, rate=sum_residual_dropout_rate, name="layer_sum_residual_dropout"
        )
        output = dr(output)
    #     output = layers.Flatten(name="Output")(
    #         layers.Dense(
    #             1,
    #             activation="sigmoid",
    #             kernel_initializer=tf.keras.initializers.HeNormal(seed=seed),
    #         )(output)
    #     )
    output = layers.Dense(
        1,
        kernel_initializer=tf.keras.initializers.HeNormal(seed=seed),
        name='predictions_dense_layer'
    )(output)
    output = layers.Activation("sigmoid", dtype="float32", name="predictions")(output)
    output = layers.Flatten(name="Output")(output)

    model = models.Model(inputs=model_input, outputs=output, name=model_name)

    return model
