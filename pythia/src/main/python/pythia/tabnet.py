import tensorflow as tf

# This is an modified tabnet without glu block, sparse max, different dim setting for the transform block (orignal one is 2 times size) and not use the mask loss

class TransformBlock(tf.keras.Model):

    def __init__(self, features,
                 momentum=0.9,
                 virtual_batch_size=None,
                 block_name='',
                 **kwargs):
        super(TransformBlock, self).__init__(**kwargs)

        self.features = features
        self.momentum = momentum
        self.virtual_batch_size = virtual_batch_size

        self.transform = tf.keras.layers.Dense(self.features, use_bias=False, name=f'transformblock_dense_{block_name}')

        # in case that the virtual_batch_size cannot work
        self.bn = tf.keras.layers.BatchNormalization(axis=-1, momentum=momentum,
                                                     virtual_batch_size=virtual_batch_size,
                                                     name=f'transformblock_bn_{block_name}')

    def call(self, inputs, training=True):
        x = self.transform(inputs)
        x = self.bn(x, training=training)
        return x


class TabNet(tf.keras.Model):

    def __init__(self, num_features,
                 feature_dim = 64,
                 output_dim  = 64,
                 num_decision_steps = 5,
                 relaxation_factor = 1.5,
                 batch_momentum = 0.98,
                 virtual_batch_size = None,
                 epsilon = 1e-5,
                 **kwargs):
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
            raise ValueError("To compute `features_for_coef`, feature_dim must be larger than output dim")

        feature_dim = int(feature_dim)
        output_dim = int(output_dim)
        num_decision_steps = int(num_decision_steps)
        relaxation_factor = float(relaxation_factor)
        batch_momentum = float(batch_momentum)
        epsilon = float(epsilon)

        if relaxation_factor < 0.:
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

        self.input_bn = tf.keras.layers.BatchNormalization(axis=-1, momentum=batch_momentum, name='input_bn')

        self.transform_f1 = TransformBlock(self.feature_dim, self.batch_momentum, self.virtual_batch_size,
                                           block_name='f1')

        self.transform_f2 = TransformBlock(self.feature_dim, self.batch_momentum, self.virtual_batch_size,
                                           block_name='f2')

        self.transform_f3_list = [
            TransformBlock(self.feature_dim, self.batch_momentum, self.virtual_batch_size,
                           block_name=f'f3_{i}')
            for i in range(self.num_decision_steps)
        ]

        self.transform_f4_list = [
            TransformBlock(self.feature_dim, self.batch_momentum, self.virtual_batch_size,
                           block_name=f'f4_{i}')
            for i in range(self.num_decision_steps)
        ]

        self.transform_coef_list = [
            TransformBlock(self.num_features,
                           self.batch_momentum, self.virtual_batch_size, block_name=f'coef_{i}')
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
        output_aggregated = tf.zeros([batch_size, self.output_dim])
        # even the beginning, the features can be considered as masked one without using mask
        masked_features = features
        # the mask for the input features
        mask_values = tf.zeros([batch_size, self.num_features])
        aggregated_mask_values = tf.zeros([batch_size, self.num_features])
        complementary_aggregated_mask_values = tf.ones(
            [batch_size, self.num_features])

        for ni in range(self.num_decision_steps):
            # Feature transformer with two shared and two decision step dependent
            # blocks is used below.=
            transform_f1 = self.transform_f1(masked_features, training=training)

            transform_f2 = self.transform_f2(transform_f1, training=training)

            transform_f2 = transform_f2 * tf.constant(0.4) + transform_f1 * tf.constant(0.8)

            transform_f3 = self.transform_f3_list[ni](transform_f2, training=training)

            transform_f3 = transform_f3 * tf.constant(0.4) + transform_f2 * tf.constant(0.8)

            transform_f4 = self.transform_f4_list[ni](transform_f3, training=training)

            transform_f4 = transform_f4 * tf.constant(0.4) + transform_f3 * tf.constant(0.8)

            if (ni > 0 or self.num_decision_steps == 1):
                decision_out = tf.nn.relu(transform_f4[:, :self.output_dim])

                # Decision aggregation.
                output_aggregated += decision_out

                # Aggregated masks are used for visualization of the
                # feature importance attributes.
                scale_agg = tf.reduce_sum(decision_out, axis=1, keepdims=True)

                if self.num_decision_steps > 1:
                    scale_agg = scale_agg / tf.cast(self.num_decision_steps - 1, tf.float32)

                aggregated_mask_values += mask_values * scale_agg

            features_for_coef = transform_f4[:, self.output_dim:]

            if ni < (self.num_decision_steps - 1):
                # Determines the feature masks via linear and nonlinear
                # transformations, taking into account of aggregated feature use.
                mask_values = self.transform_coef_list[ni](features_for_coef, training=training)
                mask_values *= complementary_aggregated_mask_values

                mask_values = tf.nn.softmax(mask_values * tf.constant(100.0), axis=-1)

                # Relaxation factor controls the amount of reuse of features between
                # different decision blocks and updated with the values of
                # coefficients.
                complementary_aggregated_mask_values *= (
                        self.relaxation_factor - mask_values)

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
