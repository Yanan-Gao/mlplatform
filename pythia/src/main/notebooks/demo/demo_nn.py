# https://github.com/tensorflow/io#tensorflow-version-compatibility
# https://github.com/tensorflow/io/issues/1731
# will be used for reading s3 with tf (but probably not needed with mnt)
#%pip install tensorflow-io==0.31.0

from collections import namedtuple, Counter
from tqdm import tqdm
import os

import pandas as pd

from sklearn.utils import class_weight

from tensorflow import keras
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, Callback
from tensorflow.keras.models import load_model, Model
from tensorflow.keras import layers
from tensorflow.keras.layers import Dense, Lambda, Input, Dropout

tf.random.set_seed(13)

# Settings about GPU if it is available
gpus = tf.config.list_physical_devices('GPU')
print("Num GPUs Available: ", len(gpus))
if gpus:
    try:
    # Currently, memory growth needs to be the same across GPUs
        #for gpu in gpus:
        #    tf.config.experimental.set_memory_growth(gpu, True)
        logical_gpus = tf.config.list_logical_devices('GPU')
        print(len(gpus), "Physical GPUs,", len(logical_gpus), "Logical GPUs")
    except RuntimeError as e:
        # Memory growth must be set before GPUs have been initialized
        print(e)

#gpus = tf.config.list_logical_devices("GPU")
# Select appropriate strategy
if len(gpus) > 1: # multiple GPUs
  #strategy = tf.distribute.MirroredStrategy()
  strategy = tf.distribute.MultiWorkerMirroredStrategy()
  print('Running on multiple GPUs ', [gpu.name for gpu in gpus])
elif len(gpus) == 1: # single GPU
  strategy = tf.distribute.get_strategy()
  print('Running on single GPU ', gpus[0].name)
else: # only CPU
  strategy = tf.distribute.get_strategy() # default strategy that works on CPU and single GPU
  print('Running on CPU')

# emb size for each categorical features; later will be specific for each
DEFAULT_EMB_DIM = 16

# see documentation in tabnet class
feature_dim_factor = 1.3
num_decision_steps = 9
dense_layer_dim = 64 # penultimate layer = embedding + tabnet, should equal output_dim
output_dim = 64
feature_dim = 64

epochs = 100
patience = 8

buffer_size = 1000000 # buffer size for 'shuffle'
batch_size = 16384 # for 'batch,' prev 10240

initializer = tf.keras.initializers.HeNormal()
# if this parameter is setted to False, we will use the model number of patience epoch after the early stop
save_best = False

# commented features may be used later, currently not available in bidImp
DEFAULT_CARDINALITIES = {
  # "TargetingDataId": 2000003,
  "SupplyVendor": 102,
  # "DealId": 20002,
  "SupplyVendorPublisherId": 200002,
  # "SupplyVendorSiteId": 102,
  "Site": 500002,
  # "AdFormat": 202,
  # "ImpressionPlacementId": 102,
  "Country": 252,
  "Region": 4002,
  "Metro": 302,
  "City": 150002,
  "Zip": 90002,
  "DeviceMake": 6002,
  "DeviceModel": 40002,
  "RequestLanguages": 1000,
  "RenderingContext": 6,
  #    "UserHourOfWeek": 24,
  #    "AdsTxtSellerType": 7,
  #    "PublisherType": 7,
  "DeviceType": 9,
  "OperatingSystemFamily": 10,
  "Browser": 20,
  "InternetConnectionType": 10,
  "ContextualCategories": 700
  # "MatchedFoldPosition": 5,
  #"LabelValue": 2
}

def emb_sz_rule(n_cat):
    "Rule of thumb to pick embedding size corresponding to `n_cat`"
    return min(DEFAULT_EMB_DIM, round(1.6 * n_cat ** 0.56))

Feature = namedtuple("Feature",
                     "name, type, cardinality, default_value, embedding_dim")

# define for the model input features
model_features = [
  Feature("SupplyVendor", tf.int32, DEFAULT_CARDINALITIES["SupplyVendor"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendor"])),
  # Feature("DealId", tf.int32, DEFAULT_CARDINALITIES["DealId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DealId"] )),
  Feature("SupplyVendorPublisherId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendorPublisherId"])),
  #    Feature("SupplyVendorSiteId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("Site", tf.int32, DEFAULT_CARDINALITIES["Site"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["Site"])),
  #    Feature("ImpressionPlacementId", tf.int32, DEFAULT_CARDINALITIES["ImpressionPlacementId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("Country", tf.int32, DEFAULT_CARDINALITIES["Country"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["Country"])),
  Feature("Region", tf.int32, DEFAULT_CARDINALITIES["Region"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["Region"])),
  Feature("Metro", tf.int32, DEFAULT_CARDINALITIES["Metro"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("City", tf.int32, DEFAULT_CARDINALITIES["City"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["City"])),
  Feature("Zip", tf.int32, DEFAULT_CARDINALITIES["Zip"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["Zip"])),
  Feature("DeviceMake", tf.int32, DEFAULT_CARDINALITIES["DeviceMake"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["DeviceMake"])),
  Feature("DeviceModel", tf.int32, DEFAULT_CARDINALITIES["DeviceModel"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["DeviceModel"])),
  Feature("RequestLanguages", tf.int32, DEFAULT_CARDINALITIES["RequestLanguages"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["RequestLanguages"])),
  Feature("RenderingContext", tf.int32, DEFAULT_CARDINALITIES["RenderingContext"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["RenderingContext"])),
  #    Feature("UserHourOfWeek", tf.int32, DEFAULT_CARDINALITIES["UserHourOfWeek"]*7+2, 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  #    Feature("AdsTxtSellerType", tf.int32, DEFAULT_CARDINALITIES["AdsTxtSellerType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  #    Feature("PublisherType", tf.int32, DEFAULT_CARDINALITIES["PublisherType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("DeviceType", tf.int32, DEFAULT_CARDINALITIES["DeviceType"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["DeviceType"])),
  Feature("OperatingSystemFamily", tf.int32, DEFAULT_CARDINALITIES["OperatingSystemFamily"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["OperatingSystemFamily"])),
  Feature("Browser", tf.int32, DEFAULT_CARDINALITIES["Browser"], 0,
          emb_sz_rule(DEFAULT_CARDINALITIES["Browser"])),
  # Feature("ContextualCategories", tf.variant, DEFAULT_CARDINALITIES["ContextualCategories"], 0, 
  #      emb_sz_rule(DEFAULT_CARDINALITIES["ContextualCategories"]))
] + [
  # Feature("AdWidthInPixels", tf.float32, 1, 0.0, None),
  # Feature("AdHeightInPixels", tf.float32, 1, 0.0, None),
  Feature("sin_hour_day", tf.float32, 1, 0.0, None),
  Feature("cos_hour_day", tf.float32, 1, 0.0, None),
  Feature("sin_hour_week", tf.float32, 1, 0.0, None),
  Feature("cos_hour_week", tf.float32, 1, 0.0, None),
  Feature("Latitude", tf.float32, 1, 0.0, None),
  Feature("Longitude", tf.float32, 1, 0.0, None)
] # 21 features

Target_ = namedtuple("Feature", "name, type, default_value, enabled, binary")

def get_targets(target):
    if not isinstance(target, list):
        target = [target]
    targets = []
    for t in target:
        targets.append(Target_(name=t, type=tf.int64, default_value=0, enabled=True, binary=False))
    return targets


# get all available files under s3 dir
def get_tfrecord_files(dir_list):
    """
    Get tfrecord file list from directory
    :param dir_ist: list of dir address on s3
    """
    if isinstance(dir_list, str): # a string?
        dir_list = [dir_list]
    files_result = []
    for dir in dir_list:
        files = tf.io.gfile.listdir(dir)
        files_result.extend([
            os.path.join(dir, f)
            for f in files
            if f.endswith(".tfrecord") or f.endswith(".gz")
        ])

    return files_result


# features parser for tfrecord
class feature_parser:
    def __init__(self, target='Gender'):
        self.Target = target
        self.feature_description = {}

        for f in model_features:
            if f.type == tf.int32:
                self.feature_description.update({f.name: tf.io.FixedLenFeature([], tf.int64, f.default_value)})
            elif f.type == tf.variant:
                # variant length generate sparse tensor
                self.feature_description.update({f.name: tf.io.VarLenFeature(tf.int64)})
            else:
                # looks like tfrecord convert the data to float32
                self.feature_description.update({f.name: tf.io.FixedLenFeature([], tf.float32, f.default_value)})
        
        model_targets = get_targets(target)
        self.feature_description.update(
            {t.name: tf.io.FixedLenFeature([], tf.int64, t.default_value) for t in model_targets})
    
    # return function for tf data to use for parsing
    def parser(self, index=False):
        # return an index (added to tfrecords by monotonically_increasing_id)
        if index:
            self.feature_description.update({'index': tf.io.FixedLenFeature([], tf.int64, 0)})

        # parse function for the tfrecorddataset
        def parse(example):
            # parse example to dict of tensor
            input_ = tf.io.parse_example(example,  # data: serial Example
                                         self.feature_description) # schema
            # tf.expand_dims: add one more axis here
            # prepare for the matrix calculation in the model part
            input_[self.Target] = tf.cast(input_[self.Target], tf.float32)
            target = tf.cast(input_.pop(self.Target), tf.float32)
            return input_, target
        
        return parse


def tfrecord_dataset(files, batch_size, map_fn, train=True, buffer_size=10000, deterministic=False):
    if train:
        return tf.data.TFRecordDataset(
            files,
            compression_type="GZIP",
        ).shuffle(
            buffer_size=buffer_size, 
            seed=None, 
            reshuffle_each_iteration=True, # shuffle data after each epoch
        ).batch(
            batch_size=batch_size,
            drop_remainder=True  # make sure each batch has the same batch size
        ).map(
            map_fn,
            num_parallel_calls=tf.data.AUTOTUNE, # parallel computing
            deterministic=deterministic # concurrency deterministic control like synchronize setting in java
        ).prefetch(tf.data.AUTOTUNE)
    else:
        return tf.data.TFRecordDataset(
            files,
            compression_type="GZIP",
        ).batch(
            batch_size=batch_size,
            drop_remainder=False,
            #deterministic=deterministic
        ).map(
            map_fn,
            num_parallel_calls=tf.data.AUTOTUNE,
            deterministic=deterministic
        ).prefetch(tf.data.AUTOTUNE)


def get_labels_pandas(dataset, col=None, verbose=False):
  '''
  from 30 minutes to 30 seconds by concatenating dfs from 
  a tfrecorddataset vs iterating one row at a time
  '''
  df_list = []
  for i_, t_ in tqdm(dataset, disable=(verbose==False)):
    dft = pd.DataFrame(t_).rename({0:'LabelValue'}, axis=1)
    if col:
      dfi = pd.DataFrame(i_)[col]
      dfit = pd.concat((dfi, dft), axis=1)
    else:
      dfit = dft
    df_list.append(dfit)
  # each batch has an index from 0-(Nbatch-1), reset the index to 0-N, drop 'index'
  return pd.concat(df_list)


def embedding(name, vocab_size=10000, emb_dim=40, dtype=tf.int32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    em = layers.Embedding(input_dim=vocab_size, output_dim=emb_dim, name=f"embedding_{name}",
                         embeddings_initializer='he_normal',) # output shape: (None,1,emb_dim)
    f = layers.Flatten(name=f"flatten_{name}") # flatten output shape: (None,1*emb_dim)
    dr = layers.Dropout(seed=None, rate=0.2, name=f"layer_{name}_dropout")
    return i, dr(f(em(i)))


def value_feature(name, dtype=tf.float32):
    i = keras.Input(shape=(1,), dtype=dtype, name=f"{name}")
    return i, i


def categorical_encoding(name, num_tokens, emb_dim=40, dtype=tf.int32):
    i = keras.Input(type_spec=tf.RaggedTensorSpec(shape=[None, None], dtype=tf.int32), name=f"{name}")
    ce = layers.CategoryEncoding(num_tokens=num_tokens, output_mode="multi_hot", name=f"multi_hot_encoding_{name}")
    
    em = layers.Embedding(input_dim=num_tokens, output_dim=emb_dim, name=f"embedding_{name}",
                         embeddings_initializer='he_normal',) # output shape: (None,1,emb_dim)
    em_sum = layers.Lambda(lambda x: tf.math.reduce_sum(x, axis=-2), name=f"lambda_reduce_sum_{name}")

    f = layers.Flatten(name=f"flatten_{name}")
    dr = layers.Dropout(seed=None, rate=0.2, name=f"layer_{name}_dropout")
    
    return i, dr(f(em_sum(em(ce(i)))))


def list_to_embedding(name, vocab_size, em_size):
    i = keras.Input(shape=(1,None), dtype=tf.int32, name=f"{name}")
    em = layers.Embedding(input_dim=vocab_size, output_dim=em_size, name=f"embedding_{name}",
                         embeddings_initializer='he_normal',)
                         # embeddings_regularizer=emb_L2)
    # use for the vary length matrix
    re = layers.Reshape(target_shape=(-1, em_size), name=f"reshape_{name}")
    dr = layers.Dropout(seed=None, rate=0.2, name=f"layer_{name}_dropout")
    return i, dr(re(em(i)))


def init_model(model_features,
               target='Gender',  
               feature_dim_factor=feature_dim_factor,
               num_decision_steps=num_decision_steps,
               dense_layer_dim=dense_layer_dim,
               output_dim=output_dim,
               feature_dim=feature_dim):

    model_input_features_tuple = [embedding(f.name, f.cardinality, f.embedding_dim, f.type) if f.type == tf.int32 
                                  else categorical_encoding(f.name, f.cardinality, f.embedding_dim, f.type) if f.type == tf.variant
                                  else value_feature(f.name)
                                  for f in model_features]
    model_input_features = [x[0] for x in model_input_features_tuple]
    model_input_layers = [x[1] for x in model_input_features_tuple]
    model_input_layers = layers.concatenate(model_input_layers)

    model_input = model_input_features
    
    emb_dr = layers.Dropout(seed=None, rate=0.4, name=f"imp{output_dim}_dropout")
    model_input_layers1 = emb_dr(Dense(output_dim, 
                                       activation=None, 
                                       kernel_initializer=initializer, 
                                       name=f"embedding{output_dim}_imp")(model_input_layers))

    tabnet = TabNet(num_features=model_input_layers.shape[-1], 
                    feature_dim=int(feature_dim_factor*feature_dim),
                    output_dim=output_dim,
                    num_decision_steps=num_decision_steps)
    tab = tabnet(model_input_layers)
    
    output = tab * tf.constant(1.0) + model_input_layers1
    # changed all initializers here to 'he_normal' instead of reusing the he_normal initialized above
    penultimate = Dense(dense_layer_dim, activation='relu', kernel_initializer=initializer)(output)
    if target == 'Age':
        output = layers.Flatten(name="Output")(
                Dense(10, activation=None, kernel_initializer=initializer)(penultimate))
    elif target == 'AgeGender':
        output = layers.Flatten(name="Output")(
                Dense(20, activation=None, kernel_initializer=initializer)(penultimate))
    elif target == 'Gender':
        output = layers.Flatten(name="Output")(
            Dense(1, activation=None, kernel_initializer=initializer)(penultimate))
    else:
        raise("Invalid value for target.")

    model = Model(
        inputs=model_input,
        outputs=output,
        name="LRWithEmbedding")
    
    return model


def get_callbacks(checkpoint_filepath, patience=2, save_best=True):

    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=patience, restore_best_weights=True)
    mc = ModelCheckpoint(checkpoint_filepath, 
                      save_weights_only=True,
                      monitor='val_loss',
                      mode='min',
                      save_best_only=save_best)
    if save_best:
        return [es, mc]
    else:
        return [es]


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
                 feature_dim=64,
                 output_dim=64,
                 num_decision_steps=5,
                 relaxation_factor=1.5,
                 batch_momentum=0.98,
                 virtual_batch_size=None,
                 epsilon=1e-5,
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

                mask_values = tf.nn.softmax(mask_values * tf.constant(100.0),axis=-1)

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

# def custom_loss(y_true, y_pred):
#     logloss = tf.keras.losses.SparseCategoricalCrossentropy(y_true, y_pred, from_logits=True)
#     mse = tf.keras.losses.MeanSquaredError(y_true, y_pred, reduction="auto", name="mean_squared_error")
#     return logloss + alpha * mse

def main(train_dir, val_dir, checkpoint_filepath, model_filepath, 
         target='Gender',
         feature_dim_factor=feature_dim_factor,
         num_decision_steps=num_decision_steps,
         dense_layer_dim=dense_layer_dim,
         output_dim=output_dim,
         feature_dim=feature_dim,
         weights=None,
         patience=patience,
         epochs=epochs):
    
    train_files = get_tfrecord_files(train_dir)
    val_files = get_tfrecord_files(val_dir)

    tf_parse = feature_parser(target)

    train = tfrecord_dataset(train_files, batch_size, tf_parse.parser(), train=True, buffer_size=buffer_size)
    val = tfrecord_dataset(val_files, batch_size*10, tf_parse.parser(), train=False)

    if weights is None:
        if target == 'Age':
            weights = {i: 1.0 for i in range(10)}
        elif target == 'Gender':
            weights = {i: 1.0 for i in range(2)}
        elif target == 'AgeGender':
            weights = {i: 1.0 for i in range(20)}
            
    print(weights)

    #with tf.device('/gpu:0'):
    #with mirrored_strategy.scope():
    with strategy.scope():
        model = init_model(model_features, target, feature_dim_factor, num_decision_steps, dense_layer_dim, output_dim, feature_dim)

        nadam = tf.keras.optimizers.Nadam(learning_rate=0.001)
        # run_opts = tf.RunOptions(report_tensor_allocations_upon_oom = True)

        if target == 'Gender':
            loss = tf.keras.losses.BinaryCrossentropy(from_logits=True)
            metrics=[tf.keras.metrics.AUC(from_logits=True)]
            activation = tf.keras.activations.deserialize('sigmoid')
        else:
            loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
            metrics=[tf.keras.metrics.SparseCategoricalAccuracy()]
            activation = tf.keras.activations.deserialize('softmax')

        model.compile(optimizer=nadam,
                      loss=loss,
                      metrics=metrics)

        # training the model
        history = model.fit(train, 
                            epochs=epochs, 
                            validation_data=val,
                            class_weight=weights,
                            callbacks=get_callbacks(checkpoint_filepath, patience=patience, save_best=save_best), 
                            verbose=2)
    
    # output layer before flatten
    model.layers[-2].activation = activation
    model.save(model_filepath)
    
    return history, model