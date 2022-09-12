import sys
from datetime import datetime, timedelta

import tensorflow as tf
from absl import app, flags

from plutus import ModelHeads, CpdType
from plutus.data import s3_sync, tfrecord_files_dict, generate_random_pandas, create_datasets_dict, get_epochs, \
    lookback_csv_dataset_generator
from plutus.feature_utils import get_features_from_json
from plutus.features import default_model_targets, get_model_targets
from plutus.losses import google_fpa_nll, google_mse_loss, google_bce_loss, mb2w_nll
from plutus.metrics import eval_model_with_anlp
from plutus.models import basic_model, dlrm_model, fastai_tabular_model, replace_last_layer
from plutus.prometheus import Prometheus

FLAGS = flags.FLAGS

FEATURES_PATH = "features.json"
INPUT_PATH = "/var/tmp/input/"
CSV_INPUT_PATH = "/var/tmp/csv_input/"
SUPPLY_VENDORS = "google,rubicon"

OUTPUT_PATH = "/var/tmp/output/"
MODEL_LOGS = "/var/tmp/logs/"
META_DATA_INPUT = "var/tmp/input"
S3_PROD = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/"
PARAM_MODEL_OUTPUT = "models_params/"
MODEL_OUTPUT = "models/"
EVAL_OUTPUT = "eval_metrics/"
S3_MODEL_LOGS = "model_logs/"

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
flags.DEFINE_string('input_path', default=INPUT_PATH,
                    help=f'Location of input files (TFRecord). Default {INPUT_PATH}')

flags.DEFINE_string('csv_input_path', default=None,
                    help=f'Location of csv input files. Default {CSV_INPUT_PATH}')

flags.DEFINE_list('sv', default=SUPPLY_VENDORS,
                  help=f'Supply vendors to train on. Default {SUPPLY_VENDORS}')

flags.DEFINE_string('meta_data_path', default=META_DATA_INPUT,
                    help=f'Location of meta data. Default {META_DATA_INPUT}')
flags.DEFINE_string('log_path', default=MODEL_LOGS, help=f'Location of model training log files. Default {MODEL_LOGS}')
flags.DEFINE_string('output_path', default=OUTPUT_PATH, help=f'Location of model output files. Default {OUTPUT_PATH}')
flags.DEFINE_string('s3_output_path', default=S3_PROD,
                    help=f'Location of S3 model output files. Default {S3_PROD}')

flags.DEFINE_boolean('push_training_logs', default=False, help=f'option to push all logs to s3 for debugging. defaulted false')

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
flags.DEFINE_boolean('run_eval', default=False, help='determine whether or not we run eval step')

# callbacks
flags.DEFINE_integer('early_stopping_patience', default=5, help='patience for early stopping', lower_bound=2)
flags.DEFINE_list("profile_batches", default=[100, 120], help="batches to profile")

# Features
flags.DEFINE_list("exclude_features", default=[], help="Features to exclude from the model")
flags.DEFINE_list("exclude_targets", default=[], help="Targets to exclude from the model")

flags.DEFINE_enum('job', 'running', ['running', 'stopped'], 'Job status.')

flags.DEFINE_string('model_training_date', default=(datetime.now() - timedelta(1)).strftime("%Y%m%d%H") ,
                    help='The last date of training data typically this is the holdout date. Training data will'
                         'consist of the days in the lookback and we use the last day as holdout validation and test'
                    )

flags.DEFINE_string('model_creation_date',
                    default=datetime.now().strftime("%Y%m%d%H%M"),
                    help='Time the model was created. Its ISO date format YYYYMMDDHHMM (e.g. 202112311411) defaults now'
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
    model_features = get_features_from_json(FEATURES_PATH, FLAGS.exclude_features)

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
        loss = google_fpa_nll if FLAGS.csv_input_path is None else mb2w_nll

    return loss


def prepare_real_data(model_features, model_targets):
    """
    create directory
    copy data
    list files
    create datasets

    """
    if FLAGS.csv_input_path is None:
        print(FLAGS.input_path)
        files = tfrecord_files_dict(FLAGS.input_path)
        print(files)
        return create_datasets_dict(files, FLAGS.batch_size, model_features, model_targets, FLAGS.eval_batch_size)
    else:
        selected_cols = {f.name: f.type for f in model_features}
        selected_cols["mb2w"] = tf.float32

        ds_tr, ds_val, ds_ts = lookback_csv_dataset_generator(
            end_date_string=FLAGS.model_training_date,
            path_prefix=[f"{FLAGS.csv_input_path}/{sv}/" for sv in FLAGS.sv],
            batch_size=FLAGS.batch_size,
            selected_cols=selected_cols,
        )
        return {
            TRAIN: ds_tr,
            VAL: ds_val,
            TEST: ds_ts,
        }


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
        epochs = get_epochs(FLAGS.meta_data_path, FLAGS.batch_size,
                            FLAGS.steps_per_epoch) if FLAGS.csv_input_path is None else FLAGS.num_epochs
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
                        steps_per_epoch=FLAGS.steps_per_epoch if FLAGS.csv_input_path is None else None,
                        verbose=FLAGS.training_verbosity,
                        callbacks=get_callbacks(),
                        validation_data=datasets[VAL],
                        validation_steps=None,
                        validation_batch_size=FLAGS.eval_batch_size)

    model_tag = f"{FLAGS.output_path}model/{FLAGS.model_arch}_{FLAGS.heads}_{FLAGS.cpd_type}_{FLAGS.dropout_rate}_{FLAGS.batchnorm}/"
    model.save(model_tag)

    params_model_tag = save_params_model(model)

    if (FLAGS.push_training_logs):
        s3_sync(FLAGS.log_path, f"{FLAGS.s3_output_path}{S3_MODEL_LOGS}")

    s3_sync(model_tag, f"{FLAGS.s3_output_path}{MODEL_OUTPUT}{FLAGS.model_creation_date}")
    s3_sync(params_model_tag, f"{FLAGS.s3_output_path}{PARAM_MODEL_OUTPUT}{FLAGS.model_creation_date}")

    epoch_gauge = Prometheus.define_gauge('epochs', 'number of epochs')
    steps_gauge = Prometheus.define_gauge('num_steps', 'number of steps per epoch')
    loss_gaug = Prometheus.define_gauge('loss', 'loss value')
    val_loss_gaug = Prometheus.define_gauge('val_loss', 'validation loss')
    steps_gauge.set(FLAGS.steps_per_epoch)
    epoch_gauge.set(epochs)
    # will likely want to change these to be series at some point
    loss_gaug.set(history.history['loss'][-1])
    val_loss_gaug.set(history.history['val_loss'][-1])

    # not currently using for anything and resulting in errors. disabling until we have time to debug
    if FLAGS.run_eval:
        print("\n evaluating model......... \n")
        # Evaluation with just ANLP now
        evals = eval_model_with_anlp(
            model=model,
            ds_test=datasets[TEST]
        )
        eval_anlp_gaug = Prometheus.define_gauge('eval_anlp', 'evaluation anlp')
        eval_anlp_gaug.set(evals[1])

    Prometheus.push()
    print("\n pushing metrics to prom \n")


def save_params_model(model):
    """
    This will remove the Distribtuion from the model and output the parameters.
    Users of this model will need to input the parameters into a library / distribution
    """
    param_model = replace_last_layer(model)
    param_model_tag = f"{FLAGS.output_path}model/{FLAGS.model_arch}_{FLAGS.heads}_{FLAGS.cpd_type}_{FLAGS.dropout_rate}_{FLAGS.batchnorm}_params"
    param_model.save(param_model_tag)
    return param_model_tag


if __name__ == '__main__':
    app.run(main)
