import sys
import time
from datetime import datetime

import tensorflow as tf
from absl import app, flags

from plutus import ModelHeads, CpdType
from plutus.data import s3_sync, list_tfrecord_files, generate_random_pandas, datasets, get_epochs
from plutus.features import default_model_features, default_model_targets, get_model_features, get_model_targets
from plutus.losses import google_fpa_nll, google_mse_loss, google_bce_loss
from plutus.metrics import evaluate_model_saving
from plutus.models import basic_model, dlrm_model, fastai_tabular_model
from plutus.prometheus import Prometheus

FLAGS = flags.FLAGS

MODEL_INPUT = "/var/tmp/input/"
MODEL_OUTPUT = "/var/tmp/output/"
MODEL_LOGS = "/var/tmp/logs/"
META_DATA_INPUT="var/tmp/input"
S3_PROD = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/"
PARAM_MODEL_OUTPUT = "models_params/"
MODEL_OUTPUT = "models/"
EVAL_OUTPUT = "eval_metrics/"

TRAIN = "train"
VAL = "validation"
TEST = "test"

# Model Choices
flags.DEFINE_string('activation', default='relu', help='activation to use in the MLP')

# DLRM
flags.DEFINE_list("bottom_mlp_units", default=[512, 256, 64, 16], help="Number of units in the bottom MLP of DLRM")
flags.DEFINE_list("top_mlp_units", default=[512, 256, 64], help="Number of units per layer in top MLP of DLRM")

# Basic
flags.DEFINE_list("basic_mlp_units", default=[512, 256, 64], help="Number of units per layer in Basic Model MLP")

# FastAI
flags.DEFINE_list("fastai_mlp_units", default=[1000, 500], help="Number of units per layer in FastAI Tabular Learner")

flags.DEFINE_enum("cpd_type", default="lognorm", enum_values=['lognorm', 'mixture', 'regression'],
                  help='The conditional probability distribution to use as output')

flags.DEFINE_enum("heads", default="cpd", enum_values=['cpd', 'cpd_floor', 'cpd_floor_win'],
                  help='Model head(s) to allow for multiple objectives')

flags.DEFINE_enum("model_arch", default="fastai", enum_values=['basic', 'dlrm', 'fastai'],
                  help='Model architecture to predict parameters for CPD')

flags.DEFINE_enum("sparse_combiner", default="flatten", enum_values=['flatten', 'global_max_pool'],
                  help='When there are multi-hot indexes for a categorical (sparse) feature, they will be combined '
                       'using this method')

flags.DEFINE_integer('num_mixture_components', default=3, help='Number of compoents for the mixture', lower_bound=2)

# Paths
flags.DEFINE_string('input_path', default=MODEL_INPUT,
                    help=f'Location of input files (TFRecord). Default {MODEL_INPUT}')
flags.DEFINE_string('meta_data_path', default=META_DATA_INPUT,
                    help=f'Location of meta data. Default {META_DATA_INPUT}')
flags.DEFINE_string('log_path', default=MODEL_LOGS, help=f'Location of model training log files. Default {MODEL_LOGS}')
flags.DEFINE_string('output_path', default=MODEL_OUTPUT, help=f'Location of model output files. Default {MODEL_OUTPUT}')
flags.DEFINE_string('s3_output_path', default=S3_MODEL_OUTPUT,
                    help=f'Location of S3 model output files. Default {S3_MODEL_OUTPUT}')

flags.DEFINE_string('log_tag', default=f"{datetime.now().strftime('%Y-%m-%d-%H')}", help='log tag')


# Learning Parameters
flags.DEFINE_float("learning_rate", default=0.0001, help="Learning Rate for optimiser")
flags.DEFINE_float("dropout_rate", default=None, help="Dropout rate for MLP")
flags.DEFINE_boolean('batchnorm', default=True, help='Apply batchnorm')

# Dummy Run
flags.DEFINE_boolean('dummy', default=True, help='dummy data used')

# Training params
flags.DEFINE_integer('batch_size', default=10, help='Batch size for training', lower_bound=1)
flags.DEFINE_integer('num_epochs', default=2, help='Number of epochs for training', lower_bound=1)
flags.DEFINE_integer('steps_per_epoch', default=2, help='Number of steps to have in each epoch of training',
                     lower_bound=1)
flags.DEFINE_integer('training_verbosity', default=1, help='Verbose levels for training (0, 1, 2)')

# Eval params
flags.DEFINE_integer('eval_batch_size', default=None, help='Batch size for evaluation')
flags.DEFINE_string('eval_model_path', default=None, help=f'Location of Model to evaluate')

# callbacks
flags.DEFINE_integer('early_stopping_patience', default=5, help='patience for early stopping', lower_bound=2)
flags.DEFINE_list("profile_batches", default=[100, 120], help="batches to profile")

# Features
flags.DEFINE_list("exclude_features", default=[], help="Features to exclude from the model")
flags.DEFINE_list("exclude_targets", default=[], help="Targets to exclude from the model")

flags.DEFINE_enum('job', 'running', ['running', 'stopped'], 'Job status.')

flags.DEFINE_string('model_creation_date',
                    default=datetime.now().strftime("%Y%m%d%H"),
                    help='Time the model was created. Its ISO date format YYYYMMDDHH (e.g. 2021123114) defaults now'
                         'Not related to the date on the training data, rather is used to identify when the model was '
                         'trained. Primary use is for model loader to load latest model to prod.')

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)


def model_builder(model_features):
    sparse_combiner = tf.keras.layers.GlobalMaxPool1D() if FLAGS.sparse_combiner == "global_max_pool" else tf.keras.layers.Flatten()

    if FLAGS.model_arch == "basic":
        model = basic_model(features=model_features,
                            activation=FLAGS.activation,
                            combiner=sparse_combiner,
                            top_mlp_layers=FLAGS.basic_mlp_units,
                            cpd_type=CpdType.from_str(FLAGS.cpd_type),
                            heads=ModelHeads.from_str(FLAGS.heads),
                            mixture_components=FLAGS.num_mixture_components,
                            dropout_rate=FLAGS.dropout_rate,
                            batchnorm=FLAGS.batchnorm)

    elif FLAGS.model_arch == "dlrm":
        model = dlrm_model(features=model_features,
                           activation=FLAGS.activation,
                           combiner=sparse_combiner,
                           top_mlp_layers=FLAGS.top_mlp_units,
                           bottom_mlp_layers=FLAGS.bottom_mlp_units,
                           cpd_type=CpdType.from_str(FLAGS.cpd_type),
                           heads=ModelHeads.from_str(FLAGS.heads),
                           mixture_components=FLAGS.num_mixture_components,
                           dropout_rate=FLAGS.dropout_rate,
                           batchnorm=FLAGS.batchnorm)

    elif FLAGS.model_arch == "fastai":
        model = fastai_tabular_model(features=model_features,
                                     activation=FLAGS.activation,
                                     combiner=sparse_combiner,
                                     layers=FLAGS.fastai_mlp_units,
                                     cpd_type=CpdType.from_str(FLAGS.cpd_type),
                                     heads=ModelHeads.from_str(FLAGS.heads),
                                     mixture_components=FLAGS.num_mixture_components,
                                     dropout_rate=FLAGS.dropout_rate,
                                     batchnorm=FLAGS.batchnorm)
    else:
        raise Exception("unknown model type")

    return model


def get_features_targets():
    model_features = get_model_features(FLAGS.exclude_features) if len(
        FLAGS.exclude_features) > 0 else default_model_features
    model_targets = get_model_targets(FLAGS.exclude_targets) if len(
        FLAGS.exclude_targets) > 0 else default_model_targets
    return model_features, model_targets


def get_loss_for_heads():
    """
    Gets the appropriate loss function for a given configuration

    CPD needs a NLL loss
    REG needs a MSE loss
    BIN needs a BCE loss

    CPD = Single Head (NLL)
    CPD_FLOOR = Dual head (NLL and MSE)
    CPD_FLOOR_WIN = Tripple Head (NLL, MSE, BIN)
    """
    if FLAGS.heads == "cpd_floor_win":
        loss = [google_fpa_nll, google_mse_loss, google_bce_loss]
    elif FLAGS.heads == "cpd_floor":
        loss = [google_fpa_nll, google_mse_loss]
    else:
        loss = google_fpa_nll

    return loss


def prepare_real_data(model_features, model_targets):
    """
    create directory
    copy data
    list files
    create datasets

    """
    print(FLAGS.input_path)
    files = list_tfrecord_files(FLAGS.input_path)
    print(files)
    return datasets(files, FLAGS.batch_size, model_features, model_targets, FLAGS.eval_batch_size)


def prepare_dummy_data(model_features, model_targets):
    """
    Used for testing. This creates a dummy dataset from random data.
    """

    def generate_tensor_slices(num):
        x, y = generate_random_pandas(model_features, model_targets, num)
        return dict(x), y

    return {
        TRAIN: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(1000)).batch(FLAGS.batch_size),
        VAL: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).batch(FLAGS.batch_size),
        TEST: tf.data.Dataset.from_tensor_slices(generate_tensor_slices(100)).batch(FLAGS.batch_size)
    }


def get_callbacks():
    tb_callback = tf.keras.callbacks.TensorBoard(log_dir=f"{FLAGS.log_path}{FLAGS.log_tag}",
                                                 write_graph=True,
                                                 profile_batch=FLAGS.profile_batches)

    es_cb = tf.keras.callbacks.EarlyStopping(patience=FLAGS.early_stopping_patience,
                                             restore_best_weights=True)

    checkpoint_base_path = f"{FLAGS.output_path}checkpoints/"
    checkpoint_filepath = checkpoint_base_path + "weights.{epoch:02d}-{val_loss:.2f}"
    chkp_cb = tf.keras.callbacks.ModelCheckpoint(
        filepath=checkpoint_filepath,
        save_weights_only=True,
        monitor='val_loss',
        mode='min',
        save_best_only=False,
        save_freq='epoch')

    return [tb_callback, es_cb, chkp_cb]


def main(argv):
    model_features, model_targets = get_features_targets()

    try:
       epochs = get_epochs(FLAGS.meta_data_path, FLAGS.batch_size, FLAGS.steps_per_epoch)
    except Exception as inst:
        print(inst.args)
        epochs = FLAGS.num_epochs

    datasets = prepare_dummy_data(model_features, model_targets) if FLAGS.dummy else prepare_real_data(model_features,
                                                                                                       model_targets)
    model = model_builder(model_features)

    model.summary()

    # BatchNorm containts non-trainable weights
    # print(model.non_trainable_weights)

    model.compile(optimizer=tf.keras.optimizers.Adam(),
                  loss=get_loss_for_heads(),
                  metrics=[],
                  loss_weights=None,
                  run_eagerly=None,
                  steps_per_execution=None)  # this could be useful

    history = model.fit(datasets[TRAIN],
                        epochs=epochs,
                        steps_per_epoch=FLAGS.steps_per_epoch,
                        verbose=FLAGS.training_verbosity,
                        callbacks=get_callbacks(),
                        validation_data=datasets[VAL],
                        validation_steps=None,
                        validation_batch_size=FLAGS.eval_batch_size)

    model_tag = f"{FLAGS.output_path}model/{FLAGS.model_arch}_{FLAGS.heads}_{FLAGS.cpd_type}_{FLAGS.dropout_rate}_{FLAGS.batchnorm}/"
    model.save(model_tag)

    params_model_tag = save_params_model(model)

    s3_sync(model_tag, f"{S3_PROD}{MODEL_OUTPUT}{FLAGS.model_creation_date}")
    s3_sync(params_model_tag, f"{S3_PROD}{PARAM_MODEL_OUTPUT}{FLAGS.model_creation_date}")

    epoch_gauge = Prometheus.define_gauge('epochs', 'number of epochs')
    steps_gauge = Prometheus.define_gauge('num_steps', 'number of steps per epoch')
    loss_gaug = Prometheus.define_gauge('loss', 'loss value')
    val_loss_gaug = Prometheus.define_gauge('val_loss', 'validation loss')
    steps_gauge.set(FLAGS.steps_per_epoch)
    epoch_gauge.set(epochs)
    # will likely want to change these to be series at some point
    loss_gaug.set(history.history['loss'][-1])
    val_loss_gaug.set(history.history['val_loss'][-1])


    df_pd = evaluate_model_saving(model=model,
                                  dataset=datasets[TEST],
                                  batch_size=FLAGS.eval_batch_size if FLAGS.eval_batch_size is not None else FLAGS.batch_size,
                                  batch_per_epoch=None)

    local_eval_metrics_path = f"{FLAGS.output_path}eval/savings.csv"
    df_pd.to_csv(local_eval_metrics_path)
    s3_sync(local_eval_metrics_path, f"{S3_PROD}{EVAL_OUTPUT}{FLAGS.model_creation_date}" )


def save_params_model(model):
    """
    This will remove the Distribtuion from the model and output the parameters.
    Users of this model will need to input the parameters into a library / distribution
    """
    model_headless = tf.keras.Model(model.inputs, model.get_layer("params").output)
    headless_model_tag = f"{FLAGS.output_path}model/{FLAGS.model_arch}_{FLAGS.heads}_{FLAGS.cpd_type}_{FLAGS.dropout_rate}_{FLAGS.batchnorm}_params"
    model_headless.save(headless_model_tag)
    return headless_model_tag


def eval():
    model_features, model_targets = get_features_targets()

    datasets = prepare_dummy_data(model_features, model_targets) if FLAGS.dummy else prepare_real_data(model_features,
                                                                                                       model_targets)

    model = tf.keras.models.load_model(FLAGS.eval_model_path)

    df_pd = evaluate_model_saving(model=model,
                                  dataset=datasets[TEST],
                                  batch_size=FLAGS.eval_batch_size if FLAGS.eval_batch_size is not None else FLAGS.batch_size,
                                  batch_per_epoch=None)

    df_pd.to_csv(f"{FLAGS.output_path}eval/savings.csv")


if __name__ == '__main__':
    app.run(main)
