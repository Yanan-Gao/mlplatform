import sys
import os
from datetime import datetime

import tensorflow as tf
import atexit
from absl import app, flags
from tensorflow.keras.layers import Embedding

from philo.data import prepare_dummy_data, prepare_real_data, s3_copy, get_steps_epochs_emr, TRAIN, VAL, TEST
from philo.data import list_tfrecord_files
from philo.layers import custom_objects
from philo.features import DEFAULT_MODEL_TARGET
from philo.models import model_builder
from philo.utils import get_callbacks
from philo.prometheus import Prometheus
from philo.neo import extract_dnn_only_models
from dalgo_utils import features

FLAGS = flags.FLAGS

INPUT_PATH = "/var/tmp/input/"
OUTPUT_PATH = "/var/tmp/output/"
MODEL_LOGS = "/var/tmp/logs/"
META_DATA_INPUT = "/var/tmp/input"
LATEST_MODEL = "/var/tmp/input"
S3_PROD = "s3://thetradedesk-mlplatform-us-east-1/models/{env}/philo/v=3/{region}/"
FEATURES_PATH = "features.json"
# PARAM_MODEL_OUTPUT = "models_params/"
MODEL_OUTPUT = "models/"
NEO_A_OUTPUT = "adgroup/"
NEO_B_OUTPUT = "bidrequest/"
BIAS_OUTPUT = "bias/"
MODEL_REGION = "namer"
EVAL_OUTPUT = "eval_metrics/"
S3_MODEL_LOGS = "model_logs/"

# TRAIN = "train"
# VAL = "validation"
# TEST = "test"

DATE_TIME = datetime.now()

flags.DEFINE_string('env', default='dev', help=f'Execution environment (dev, test, prod)')

# define model structure
flags.DEFINE_string("model_arch", default="deepfm_dual", help="Model Architecture, dual DNN towers")
flags.DEFINE_list("dnn_hidden_units", default=[[64, 64], [128, 64]], help="Number of units in the deep tower")
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
# blank region supports existing regionless training
flags.DEFINE_string('region', default='', help=f'Region for the model data. Default blank {MODEL_REGION}')

flags.DEFINE_boolean('push_training_logs', default=False,
                     help=f'option to push all logs to s3 for debugging. defaulted false')

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

flags.DEFINE_integer('training_verbosity', default=2, help='Verbose levels for training (0, 1, 2)')

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
                    help='Time the model was created. Its ISO date format YYYYMMDDHHMM (e.g. 202112311459) defaults now'
                         'Not related to the date on the training data, rather is used to identify when the model was '
                         'trained. Primary use is for model loader to load latest model to prod.')

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)


def main(argv):
    neo_features = features.Features.from_json_path(FEATURES_PATH , FLAGS.exclude_features)

    # convert model features into the same List[nameTuple] format
    model_features = []
    adgroup_feature_list = []

    for f in neo_features.feature_definitions.ad_group:
        adgroup_feature_list.append(f.name)

    for f in neo_features.feature_definitions.flat_features():
        model_features.append(f.as_namedtuple())

    model_target = DEFAULT_MODEL_TARGET

    # define training metrics
    prom = Prometheus(job_name='philo_neo', application='modelTraining', environment=FLAGS.env)
    epoch_gauge = prom.define_gauge('epochs', 'number of epochs', ['region'])
    loss_gauge = prom.define_gauge('loss', 'loss value', ['region', 'model'])
    val_loss_gauge = prom.define_gauge('val_loss', 'validation loss', ['region', 'model'])
    eval_philo_gauge = prom.define_gauge('eval', 'evaluation', ['region', 'model'])

    # if the training process need to go through the whole dataset in more than 1 epoch
    # we need to repeat the data and get the steps_per_epochs so that tf knows how the
    # data could be digested
    repeat = True if FLAGS.data_trunks > 1 else False
    steps_per_epoch, epochs = get_steps_epochs_emr(FLAGS.meta_data_path, FLAGS.batch_size, FLAGS.data_trunks,
                                                   FLAGS.num_epochs)
    #########################################################
    print("##########################steps_per_epoch, epochs##########################")
    print(steps_per_epoch, epochs)
    print("##########################files for each directory#########################")
    files = list_tfrecord_files(FLAGS.input_path)
    for k, f in files.items():
        print(f"{k} contains {len(f)} files")
    if FLAGS.dummy:
        print("this is dummy file")
    #########################################################

    datasets = prepare_dummy_data(
        model_features, model_target, 32
    ) if FLAGS.dummy else prepare_real_data(
        model_features=model_features, model_target=model_target, input_path=FLAGS.input_path,
        batch_size=FLAGS.batch_size, eval_batch_size=FLAGS.eval_batch_size, prefetch_num=tf.data.AUTOTUNE,
        repeat=repeat)

    mirrored_strategy = tf.distribute.MultiWorkerMirroredStrategy()
    num_gpus = mirrored_strategy.num_replicas_in_sync
    print("##########################checking gpu devices#############################")
    print('Number of devices: {}'.format(num_gpus))

    # Tensorflow has a bad exist because it creats a threadpool that it doesn't ever close
    # (https://github.com/tensorflow/tensorflow/issues/50487#issuecomment-997304668)
    # Future versions of TF might fix this bugs and we can remove it, until then, use atexit to run the thread pool
    # close functions before Python exit
    # TODO: remove when theres a tf patch to resolve this

    # # Register the `_cross_device_ops._pool` and `_host_cross_device_ops._pool`s `.close()` function to run when
    # # Python exists
    # atexit.register(mirrored_strategy._extended._cross_device_ops._pool.close)
    # atexit.register(mirrored_strategy._extended._host_cross_device_ops._pool.close)
    # with mirrored_strategy.scope():
    #     # currently not using the latest model, but can be used in the future
    #     # try:
    #     #     model = tf.keras.models.load_model(FLAGS.latest_model_path, custom_objects=custom_objects)
    #     # except OSError as error:
    #     # if no model file, create a new model from scratch
    #     # print(error)
    #     kwargs = {"dnn_hidden_units": tuple(FLAGS.dnn_hidden_units), "l2_reg_linear": FLAGS.l2_reg_linear,
    #               "l2_reg_embedding": FLAGS.l2_reg_embedding, "l2_reg_dnn": FLAGS.l2_reg_dnn,
    #               "dnn_dropout": FLAGS.dnn_dropout, "dnn_activation": FLAGS.dnn_activation,
    #               "dnn_use_bn": FLAGS.dnn_use_bn, "adgroup_feature_list": adgroup_feature_list}
    #     print('Number of devices: %d' % mirrored_strategy.num_replicas_in_sync)
    #     model_1 = model_builder(FLAGS.model_arch, model_features, **kwargs)
    #
    #     auc = tf.keras.metrics.AUC()
    # TODO: include later with multi gpu training

    kwargs = {"dnn_hidden_units": tuple(FLAGS.dnn_hidden_units), "l2_reg_linear": FLAGS.l2_reg_linear,
              "l2_reg_embedding": FLAGS.l2_reg_embedding, "l2_reg_dnn": FLAGS.l2_reg_dnn,
              "dnn_dropout": FLAGS.dnn_dropout, "dnn_activation": FLAGS.dnn_activation,
              "dnn_use_bn": FLAGS.dnn_use_bn, "adgroup_feature_list": adgroup_feature_list}
    print('Number of devices: %d' % mirrored_strategy.num_replicas_in_sync)
    model_1 = model_builder(FLAGS.model_arch, model_features, **kwargs)

    auc = tf.keras.metrics.AUC()

    # step 1: train a deepfm_dual model, which has the dual DNN tower structure
    # this step is the same as the original philo alpha
    print("##########################step 1: model summary###################################")
    model_1.summary()

    model_1.compile(tf.keras.optimizers.Adam(learning_rate=FLAGS.learning_rate),
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])

    checkpoint_base_path_1 = f"{FLAGS.output_path}/step_1/checkpoints/"
    print("##########################step 1: start model training#############################")
    history_1 = model_1.fit(datasets[TRAIN],
                            epochs=epochs,
                            steps_per_epoch=steps_per_epoch,
                            verbose=FLAGS.training_verbosity,
                            callbacks=get_callbacks(f"{FLAGS.log_path}step_1/", FLAGS.profile_batches,
                                                    FLAGS.early_stopping_patience, checkpoint_base_path_1),
                            validation_data=datasets[VAL],
                            validation_steps=None,
                            validation_batch_size=FLAGS.eval_batch_size)
    print("##########################step 1: end of model training############################")

    model_tag_1 = f"{FLAGS.output_path}model/step_1/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}"

    print("##########################step 1: saving model#####################################")
    model_1.save(model_tag_1)

    # step 2: freezing the embeddings trained, retrain the model with only dual DNN towers
    # this step ensures that the DNN part will learn the embeddings to predict, unfreezing the whole model will lead to
    # the model struggling ~ 0.5 AUC
    model_2, model_neo_a, model_neo_b = extract_dnn_only_models(model_1)

    # freeze embedding layers
    for layer in list(filter(lambda x: isinstance(x, Embedding), model_2.layers)):
        layer.trainable = False

    print("##########################step 2: model summary###################################")
    model_2.summary()

    model_2.compile(tf.keras.optimizers.Adam(learning_rate=FLAGS.learning_rate),
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])

    checkpoint_base_path_2 = f"{FLAGS.output_path}/step_2/checkpoints/"

    print("##########################step 2: start model training#############################")
    _ = model_2.fit(datasets[TRAIN],
                    epochs=epochs,
                    steps_per_epoch=steps_per_epoch,
                    verbose=FLAGS.training_verbosity,
                    callbacks=get_callbacks(f"{FLAGS.log_path}step_2/", FLAGS.profile_batches,
                                            FLAGS.early_stopping_patience, checkpoint_base_path_2),
                    validation_data=datasets[VAL],
                    validation_steps=None,
                    validation_batch_size=FLAGS.eval_batch_size)
    print("##########################step 2: end of model training############################")

    model_tag_2 = f"{FLAGS.output_path}model/step_2/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}"
    print("##########################step 2: saving model#####################################")
    model_2.save(model_tag_2)

    # step 3: unfreezing the DNN only model trained, fine tune the model to improve performance
    # in this step, a smaller learning rate of 1/10 of the origincal will be used

    # unfreeze the DNN only model
    model_2.trainable = True

    print("##########################step 3: model summaries###################################")
    model_2.summary()
    model_neo_a.summary()
    model_neo_b.summary()

    model_2.compile(tf.keras.optimizers.Adam(learning_rate=FLAGS.learning_rate / 10),  # much lower learning rate
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])

    checkpoint_base_path_3 = f"{FLAGS.output_path}/step_3/checkpoints/"

    print("##########################step 3: start model training#############################")
    history_3 = model_2.fit(datasets[TRAIN],
                            epochs=epochs,
                            steps_per_epoch=steps_per_epoch,
                            verbose=FLAGS.training_verbosity,
                            callbacks=get_callbacks(f"{FLAGS.log_path}step_3/", FLAGS.profile_batches,
                                                    FLAGS.early_stopping_patience, checkpoint_base_path_3),
                            validation_data=datasets[VAL],
                            validation_steps=None,
                            validation_batch_size=FLAGS.eval_batch_size)
    print("##########################step 3: end of model training############################")

    model_tag_3_full = f"{FLAGS.output_path}model/step_3/full/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}"
    model_tag_3_neo_a = f"{FLAGS.output_path}model/step_3/neo_a/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}_neo_a"
    model_tag_3_neo_b = f"{FLAGS.output_path}model/step_3/neo_b/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}_neo_b"
    print("##########################step 3: saving models#####################################")
    model_2.save(model_tag_3_full)
    model_neo_a.save(model_tag_3_neo_a)
    model_neo_b.save(model_tag_3_neo_b)

    # save the weight of the prediction layer of the final tuned model
    # this serves as the bias term added to the product of Neo outputs, which altogether will feed to the sigmoid
    # function for the final prediction of probability
    bias_term = model_2.layers[-1].get_weights()[0][0]
    bias_tag = f"{FLAGS.output_path}model/step_3/bias/{FLAGS.model_arch}_{FLAGS.num_epochs}_{FLAGS.dnn_dropout}_{FLAGS.dnn_use_bn}.csv"
    os.makedirs(os.path.dirname(bias_tag), exist_ok=True)
    with open(bias_tag, 'w') as f:
        f.write(str(bias_term))

    # save to s3
    base_s3_path = FLAGS.s3_output_path.format(env=FLAGS.env, region=FLAGS.region)

    # push logs if needed
    if FLAGS.push_training_logs:
        path = f"{base_s3_path}{S3_MODEL_LOGS}{FLAGS.model_creation_date}"
        print(f"##########################writing logs to {path}...#######################")
        s3_copy(FLAGS.log_path, path)

    # copy model outputs to final location
    print(f"##########################writing models to {base_s3_path}...######################")
    path_philo = f"{base_s3_path}{MODEL_OUTPUT}{FLAGS.model_creation_date}"
    path_neo_a = f"{base_s3_path}{NEO_A_OUTPUT}{FLAGS.model_creation_date}"
    path_neo_b = f"{base_s3_path}{NEO_B_OUTPUT}{FLAGS.model_creation_date}"
    path_bias = f"{base_s3_path}{BIAS_OUTPUT}{FLAGS.model_creation_date}"
    print(f"Writing Philo model to {path_philo}...")
    s3_copy(f"{FLAGS.output_path}model/step_1", path_philo)
    print(f"Writing Neo AdGroup model to {path_neo_a}...")
    s3_copy(f"{FLAGS.output_path}model/step_3/neo_a", path_neo_a)
    print(f"Writing Neo BidRequest model to {path_neo_b}...")
    s3_copy(f"{FLAGS.output_path}model/step_3/neo_b", path_neo_b)
    print(f"Writing bias term to {path_bias}...")
    s3_copy(f"{FLAGS.output_path}model/step_3/bias", path_bias)

    print("##########################logging model metrics########################")
    epoch_gauge.labels(region=FLAGS.region).set(epochs)
    loss_gauge.labels(region=FLAGS.region, model="philo").set(history_1.history['loss'][-1])
    loss_gauge.labels(region=FLAGS.region, model="neo").set(history_3.history['loss'][-1])
    val_loss_gauge.labels(region=FLAGS.region, model="philo").set(history_1.history['val_loss'][-1])
    val_loss_gauge.labels(region=FLAGS.region, model="neo").set(history_3.history['val_loss'][-1])

    print("##########################evaluating model on TEST########################")
    evals_1 = model_1.evaluate(datasets[TEST], verbose=2)
    evals_3 = model_2.evaluate(datasets[TEST], verbose=2)
    eval_philo_gauge.labels(region=FLAGS.region, model="philo").set(evals_1[2])
    eval_philo_gauge.labels(region=FLAGS.region, model="neo").set(evals_3[2])

    # push metrics before stopping the job
    prom.push()


if __name__ == '__main__':
    app.run(main)
