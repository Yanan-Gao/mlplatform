import sys
from datetime import datetime

import tensorflow as tf
from absl import app, flags

from philo.data import prepare_dummy_data, prepare_real_data, s3_sync, get_steps_epochs_emr, TRAIN, VAL, TEST
from philo.features import DEFAULT_MODEL_TARGET
from philo.models import model_builder
from philo.utils import get_callbacks
from philo.prometheus import Prometheus
from philo.feature_utils import get_features_from_json

FLAGS = flags.FLAGS

INPUT_PATH = "/var/tmp/input/"
OUTPUT_PATH = "/var/tmp/output/"
MODEL_LOGS = "/var/tmp/logs/"
META_DATA_INPUT = "/var/tmp/input"
LATEST_MODEL = "/var/tmp/input"
FEATURES_PATH = "features.json"
S3_PROD = "s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/prod/"
# PARAM_MODEL_OUTPUT = "models_params/"
MODEL_OUTPUT = "models/"
EVAL_OUTPUT = "eval_metrics/"

# TRAIN = "train"
# VAL = "validation"
# TEST = "test"

DATE_TIME = datetime.now()

# define model structure
flags.DEFINE_string("model_arch", default="deepfm", help="Model Architecture, currently only have deep fm")
flags.DEFINE_list("dnn_hidden_units", default=[128, 128], help="Number of units in the deep tower")
flags.DEFINE_string('dnn_activation', default="relu", help='dnn activation function')
flags.DEFINE_float('l2_reg_linear', default=0.00001, help='linear layer l2 regularization weights')
flags.DEFINE_float('l2_reg_embedding', default=0.00001, help='embedding layer l2 regularization weights')
flags.DEFINE_float('l2_reg_dnn', default=0, help='dnn tower l2 regularization weights')
flags.DEFINE_float('dnn_dropout', default=0, help='dnn tower drop out rate')
flags.DEFINE_boolean('dnn_use_bn', default=False, help='Apply batch normalization')

# Paths
flags.DEFINE_string('input_path', default=INPUT_PATH,
                    help=f'Location of input files (TFRecord). Default {INPUT_PATH}')
flags.DEFINE_string('meta_data_path', default=META_DATA_INPUT,
                    help=f'Location of meta data. Default {META_DATA_INPUT}')
flags.DEFINE_string('latest_model_path', default=LATEST_MODEL,
                    help=f'Location of latest (previous) trained model. Default {LATEST_MODEL}')
flags.DEFINE_string('log_path', default=MODEL_LOGS, help=f'Location of model training log files. Default {MODEL_LOGS}')
flags.DEFINE_string('output_path', default=OUTPUT_PATH, help=f'Location of model output files. Default {OUTPUT_PATH}')
flags.DEFINE_string('s3_output_path', default=S3_PROD,
                    help=f'Location of S3 model output files. Default {S3_PROD}')

flags.DEFINE_string('log_tag', default=f"{DATE_TIME.strftime('%Y-%m-%d-%H-%M')}", help='log tag')

# Learning Parameters
flags.DEFINE_float("learning_rate", default=0.0001, help="Learning Rate for optimiser")

# Dummy Run
flags.DEFINE_boolean('dummy', default=True, help='dummy data used')

# Training params
flags.DEFINE_integer('batch_size', default=1024, help='Batch size for training', lower_bound=1)
flags.DEFINE_integer('num_epochs', default=10, help='Number of epochs for training, if data is broken into trunks, '
                                                    'trunk*epochs', lower_bound=1)
flags.DEFINE_integer('data_trunks', default=3, help='number of epochs to run one whole data', lower_bound=1)

flags.DEFINE_integer('training_verbosity', default=1, help='Verbose levels for training (0, 1, 2)')

# Eval params
flags.DEFINE_integer('eval_batch_size', default=None, help='Batch size for evaluation')
flags.DEFINE_string('eval_model_path', default=None, help=f'Location of Model to evaluate')

# callbacks
flags.DEFINE_integer('early_stopping_patience', default=5, help='patience for early stopping', lower_bound=2)
flags.DEFINE_list("profile_batches", default=[100, 120], help="batches to profile")

# Features
flags.DEFINE_list("exclude_features", default=[], help="Features to exclude from the model")

flags.DEFINE_enum('job', 'running', ['running', 'stopped'], 'Job status.')

flags.DEFINE_string('model_creation_date',
                    default=DATE_TIME.strftime("%Y%m%d%H%M"),
                    help='Time the model was created. Its ISO date format YYYYMMDDHHMM (e.g. 2021123114) defaults now'
                         'Not related to the date on the training data, rather is used to identify when the model was '
                         'trained. Primary use is for model loader to load latest model to prod.')

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)


def main(argv):
    model_features = get_features_from_json(FEATURES_PATH, FLAGS.exclude_features)
    model_target = DEFAULT_MODEL_TARGET

    # if the training process need to go through the whole dataset in more than 1 epoch
    # we need to repeat the data and get the steps_per_epochs so that tf knows how the
    # data could be digested
    repeat = True if FLAGS.data_trunks > 1 else False
    steps_per_epoch, epochs = get_steps_epochs_emr(FLAGS.meta_data_path, FLAGS.batch_size, FLAGS.data_trunks,
                                                   FLAGS.num_epochs)
    datasets = prepare_dummy_data(
        model_features, model_target, 32
    ) if FLAGS.dummy else prepare_real_data(
        model_features=model_features, model_target=model_target, input_path=FLAGS.input_path,
        batch_size=FLAGS.batch_size, eval_batch_size=FLAGS.eval_batch_size, prefetch_num=tf.data.AUTOTUNE,
        repeat=repeat)

    kwargs = {"dnn_hidden_units": tuple(FLAGS.dnn_hidden_units), "l2_reg_linear": FLAGS.l2_reg_linear,
              "l2_reg_embedding": FLAGS.l2_reg_embedding, "l2_reg_dnn": FLAGS.l2_reg_dnn,
              "dnn_dropout": FLAGS.dnn_dropout, "dnn_activation": FLAGS.dnn_activation, "dnn_use_bn": FLAGS.dnn_use_bn}

    model = model_builder(FLAGS.model_arch, model_features, **kwargs)

    model.summary()

    model.compile("adam",
                  "binary_crossentropy",
                  metrics=['binary_crossentropy', tf.keras.metrics.AUC()])

    checkpoint_base_path = f"{FLAGS.output_path}checkpoints/"

    history = model.fit(datasets[TRAIN],
                        epochs=epochs,
                        steps_per_epoch=steps_per_epoch,
                        verbose=FLAGS.training_verbosity,
                        callbacks=get_callbacks(FLAGS.log_path, FLAGS.profile_batches,
                                                FLAGS.early_stopping_patience, checkpoint_base_path),
                        validation_data=datasets[VAL],
                        validation_steps=None,
                        validation_batch_size=FLAGS.eval_batch_size)

    model_tag = f"{FLAGS.output_path}model/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}"
    model.save(model_tag)

    s3_sync(model_tag, f"{FLAGS.s3_output_path}{MODEL_OUTPUT}{FLAGS.model_creation_date}")

    epoch_gauge = Prometheus.define_gauge('epochs', 'number of epochs')
    loss_gauge = Prometheus.define_gauge('loss', 'loss value')
    val_loss_gauge = Prometheus.define_gauge('val_loss', 'validation loss')
    epoch_gauge.set(epochs)
    loss_gauge.set(history.history['loss'][-1])
    val_loss_gauge.set(history.history['val_loss'][-1])

    evals = model.evaluate(datasets[TEST], verbose=1)
    eval_philo_gauge = Prometheus.define_gauge('eval', 'evaluation')
    eval_philo_gauge.set(evals[2])


if __name__ == '__main__':
    app.run(main)
