import sys
import os
from datetime import datetime

import tensorflow as tf
import atexit
from absl import app, flags
from tensorflow.keras.layers import Embedding

from philo.data import prepare_dummy_data, prepare_real_data, s3_copy, s3_write_success_file, get_steps_epochs_emr, \
    TRAIN, VAL, TEST
from philo.data import list_tfrecord_files
from philo.features import DEFAULT_MODEL_TARGET
from philo.models import model_builder
from philo.utils import get_callbacks, read_yaml, save_yaml
from philo.prometheus import Prometheus
from philo.neo import extract_dnn_only_models
from dalgo_utils import features

DATE_TIME = datetime.now()
FLAGS = flags.FLAGS
FEATURES_PATH = "features.json"
PARAMS_PATH = "config.yml"
KEY_NAME = "region_specific_params"

# info that are not in the config.yml
# blank region supports existing regionless training
# Dummy Run
flags.DEFINE_boolean('dummy', default=True, help='dummy data used')
flags.DEFINE_enum('job', 'running', ['running', 'stopped'], 'Job status.')
flags.DEFINE_string('model_creation_date',
                    default=DATE_TIME.strftime('%Y%m%d%H%M'),
                    help='Time the model was created. Its ISO date format YYYYMMDDHHMM (e.g. 202112311459) defaults now'
                         'Not related to the date on the training data, rather is used to identify when the model was '
                         'trained. Primary use is for model loader to load latest model to prod.')
# flags.DEFINE_string('log_tag', default=f"{DATE_TIME.strftime('%Y-%m-%d-%H-%M')}",
#                     help='log tag')
flags.DEFINE_string('region', default=None, help=f'Region for the model data. Default blank namer')
flags.DEFINE_string('env', default='dev', help=f'Execution environment (dev, test, prod)')
flags.mark_flag_as_required('region')
# Paths, for fast test_purpose, we keep this for potential user defined variables
flags.DEFINE_string('s3_output_path', default=None,
                    help=f'Location of S3 model output files. If not set, use from config.yaml')
# Training params, for fast model tuning, keep batch_size, learning_rate and num_epochs for user defined
flags.DEFINE_integer('batch_size', default=None,
                     help='Batch size for training. If not set, use from config.yaml')
flags.DEFINE_float('learning_rate', default=None,
                   help='Learning Rate for optimiser. If not set, use from config.yaml')
flags.DEFINE_integer('num_epochs', default=None,
                     help='Number of epochs for training, if data is broken into trunks, '
                          'trunk*epochs. If not set, use from config.yaml')
app.define_help_flags()
app.parse_flags_with_usage(sys.argv)


def main(argv):
    params = read_yaml(PARAMS_PATH, FLAGS.region, KEY_NAME)
    model_name = params["model_name"]
    model_version = params["model_version"]
    train_params = params['train_params']
    model_params = params['model_params']
    input_defaults = params['input_defaults']
    output_defaults = params['output_defaults']

    # 2. setup output paths
    s3_output_path = FLAGS.s3_output_path if FLAGS.s3_output_path else output_defaults["s3_output_path"]
    batch_size = FLAGS.batch_size if FLAGS.batch_size else train_params["batch_size"]
    learning_rate = FLAGS.learning_rate if FLAGS.learning_rate else train_params["learning_rate"]
    num_epochs = FLAGS.num_epochs if FLAGS.num_epochs else train_params["num_epochs"]
    # BIAS_OUTPUT = "bias/" also in adgroup folder
    # update the param settings for the current training so that it could be later saved to s3
    output_defaults['s3_output_path'] = s3_output_path
    train_params['batch_size'] = batch_size
    train_params['learning_rate'] = learning_rate
    train_params['num_epochs'] = num_epochs
    save_yaml(output_defaults['config_save_path'],
              {'model_name': model_name,
               'model_version': model_version,
               'model_params': model_params,
               'train_params': train_params})
    # define paths for docker local
    # check points for model training process
    checkpoint_base_path_1 = f"{output_defaults['output_path']}/step_1/checkpoints/"
    checkpoint_base_path_2 = f"{output_defaults['output_path']}/step_2/checkpoints/"
    checkpoint_base_path_3 = f"{output_defaults['output_path']}/step_3/checkpoints/"
    # mode save path name
    model_tag_1 = f"{output_defaults['output_path']}model/step_1/{model_params['model_arch']}_" \
                  f"{num_epochs}_{model_params['dnn_dropout']}_{model_params['dnn_use_bn']}"
    model_tag_2 = f"{output_defaults['output_path']}model/step_2/{model_params['model_arch']}_{num_epochs}_" \
                  f"{model_params['dnn_dropout']}_{model_params['dnn_use_bn']}"
    model_tag_3_full = f"{output_defaults['output_path']}model/step_3/full/{model_params['model_arch']}_" \
                       f"{num_epochs}_{model_params['dnn_dropout']}_{model_params['dnn_use_bn']}"
    model_tag_3_neo_a = f"{output_defaults['output_path']}model/step_3/neo_a/{model_params['model_arch']}_" \
                        f"{num_epochs}_{model_params['dnn_dropout']}_{model_params['dnn_use_bn']}_neo_a"
    model_tag_3_neo_b = f"{output_defaults['output_path']}model/step_3/neo_b/{model_params['model_arch']}_" \
                        f"{num_epochs}_{model_params['dnn_dropout']}_{model_params['dnn_use_bn']}_neo_b"
    bias_tag = f"{output_defaults['output_path']}model/step_3/bias/bias.csv"
    os.makedirs(os.path.dirname(bias_tag), exist_ok=True)

    # define paths for s3 push
    base_s3_path = output_defaults['base_s3_path'].format(s3_output_path=s3_output_path,
                                                          env=FLAGS.env,
                                                          model_name=model_name,
                                                          model_version=model_version,
                                                          region=FLAGS.region)
    # s3 path for model created during each step of training
    path_combined_1 = f"{base_s3_path}{output_defaults['model_output_step_1']}{output_defaults['s3_model_files']}" \
                      f"{FLAGS.model_creation_date}"
    path_combined_2 = f"{base_s3_path}{output_defaults['model_output_step_2']}{output_defaults['s3_model_files']}" \
                      f"{FLAGS.model_creation_date}"
    path_combined_3 = f"{base_s3_path}{output_defaults['model_output_step_3']}{output_defaults['s3_model_files']}" \
                      f"{FLAGS.model_creation_date}"
    path_neo_a = f"{base_s3_path}{output_defaults['neo_a_output']}{output_defaults['s3_model_files']}" \
                 f"{FLAGS.model_creation_date}"
    path_neo_b = f"{base_s3_path}{output_defaults['neo_b_output']}{output_defaults['s3_model_files']}" \
                 f"{FLAGS.model_creation_date}"
    # s3 path for model log output
    path_log_1 = f"{base_s3_path}{output_defaults['model_output_step_1']}{output_defaults['s3_model_logs']}" \
                 f"{FLAGS.model_creation_date}"
    path_log_2 = f"{base_s3_path}{output_defaults['model_output_step_2']}{output_defaults['s3_model_logs']}" \
                 f"{FLAGS.model_creation_date}"
    path_log_3 = f"{base_s3_path}{output_defaults['model_output_step_3']}{output_defaults['s3_model_logs']}" \
                 f"{FLAGS.model_creation_date}"
    # s3 path for feature settings
    path_featuresjson_a = f"{path_neo_a}/features.json"
    path_featuresjson_b = f"{path_neo_b}/features.json"
    # s3 path for model config
    path_config = f"{base_s3_path}{output_defaults['s3_model_config']}/{FLAGS.model_creation_date}/" \
                  f"{output_defaults['config_save_path']}"
    # end of model path setting
    ############################################################
    # model feature settings
    # convert model features into the same List[nameTuple] format
    neo_features = features.Features.from_json_path(FEATURES_PATH, model_params['exclude_features'])
    model_features = []
    adgroup_feature_list = []

    for f in neo_features.feature_definitions.ad_group:
        adgroup_feature_list.append(f.name)

    for f in neo_features.feature_definitions.flat_features():
        model_features.append(f.as_namedtuple())

    model_target = DEFAULT_MODEL_TARGET
    ############################################################
    # define training metrics
    prom = Prometheus(job_name='philo_neo', application='modelTraining', environment=FLAGS.env)
    epoch_gauge = prom.define_gauge('epochs', 'number of epochs', ['region'])
    loss_gauge = prom.define_gauge('loss', 'loss value', ['region', 'model'])
    val_loss_gauge = prom.define_gauge('val_loss', 'validation loss', ['region', 'model'])
    eval_philo_gauge = prom.define_gauge('eval', 'evaluation', ['region', 'model'])

    #########################################################
    mirrored_strategy = tf.distribute.MultiWorkerMirroredStrategy()
    num_gpus = mirrored_strategy.num_replicas_in_sync

    print("##########################checking gpu devices#############################")
    print('Number of devices: {}'.format(num_gpus))

    print("##########################steps_per_epoch, epochs##########################")
    # if the training process need to go through the whole dataset in more than 1 epoch
    # we need to repeat the data and get the steps_per_epochs so that tf knows how the
    # data could be digested
    repeat = True if train_params['data_trunks'] > 1 else False
    if FLAGS.dummy:
        print("this is dummy file")
        steps_per_epoch = 10
        epochs = 1
    else:
        steps_per_epoch, epochs = get_steps_epochs_emr(input_defaults['meta_data_input'], batch_size * num_gpus,
                                                       train_params['data_trunks'], num_epochs)
    print(steps_per_epoch, epochs)
    # since batch size is distributed equally across gpus, set equal to batch_size*num_gpus
    datasets = prepare_dummy_data(
        model_features, model_target, 32
    ) if FLAGS.dummy else prepare_real_data(
        model_features=model_features, model_target=model_target, input_path=input_defaults['input_path'],
        batch_size=batch_size * num_gpus, eval_batch_size=train_params['eval_batch_size'],
        prefetch_num=tf.data.AUTOTUNE, repeat=repeat, data_format=input_defaults['data_format'])

    # Tensorflow has a bad exist because it creats a threadpool that it doesn't ever close
    # (https://github.com/tensorflow/tensorflow/issues/50487#issuecomment-997304668)
    # Future versions of TF might fix this bugs and we can remove it, until then, use atexit to run the thread pool
    # close functions before Python exit
    # TODO: remove when theres a tf patch to resolve this

    # # Register the `_cross_device_ops._pool` and `_host_cross_device_ops._pool`s `.close()` function to run when
    # # Python exists
    atexit.register(mirrored_strategy._extended._cross_device_ops._pool.close)
    atexit.register(mirrored_strategy._extended._host_cross_device_ops._pool.close)

    # step 1
    # train a deepfm_dual model, which has the dual DNN tower structure
    # this step is the same as the original philo alpha
    with mirrored_strategy.scope():
        # currently, not using the latest model, but can be used in the future
        # try:
        #     model = tf.keras.models.load_model(INPUT_DEFAULTS['latest_model_path'], custom_objects=custom_objects)
        # except OSError as error:
        # if no model file, create a new model from scratch
        # print(error)

        kwargs = {"dnn_hidden_units": tuple(model_params["dnn_hidden_units"]),
                  "l2_reg_linear": model_params["l2_reg_linear"],
                  "l2_reg_embedding": model_params["l2_reg_embedding"],
                  "l2_reg_dnn": model_params["l2_reg_dnn"],
                  "dnn_dropout": model_params["dnn_dropout"],
                  "dnn_activation": model_params["dnn_activation"],
                  "dnn_use_bn": model_params["dnn_use_bn"],
                  "adgroup_feature_list": model_params["adgroup_feature_list"],
                  'step': 1}
        print('Number of devices: %d' % mirrored_strategy.num_replicas_in_sync)
        model_1 = model_builder(model_params['model_arch'], model_features, **kwargs)

        auc = tf.keras.metrics.AUC()
    print("##########################step 1: model summary###################################")
    model_1.summary()

    model_1.compile(tf.keras.optimizers.Adam(learning_rate=learning_rate),
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])

    print("##########################step 1: start model training#############################")
    history_1 = model_1.fit(datasets[TRAIN],
                            epochs=epochs,
                            steps_per_epoch=steps_per_epoch,
                            verbose=train_params['training_verbosity'],
                            callbacks=get_callbacks(f"{output_defaults['log_path']}"
                                                    f"{output_defaults['model_output_step_1']}",
                                                    train_params['profile_batches'],
                                                    train_params['early_stopping_patience'],
                                                    checkpoint_base_path_1),
                            validation_data=datasets[VAL],
                            validation_steps=None,
                            validation_batch_size=train_params['eval_batch_size'])
    print("##########################step 1: end of model training, saving model##############")
    model_1.save(model_tag_1)

    # step 2
    # freezing the embeddings trained, retrain the model with only dual DNN towers
    # this step ensures that the DNN part will learn the embeddings to predict, unfreezing the whole model will
    # lead to the model struggling ~ 0.5 AUC
    with mirrored_strategy.scope():
        model_2, model_neo_a, model_neo_b = extract_dnn_only_models(
            model_1, task='binary', adgroup_feature_list=model_params['adgroup_feature_list'], step=-1,
            prediction_layer_name=model_params['bidder_prediction_layer'])
        # freeze embedding layers
        for layer in list(filter(lambda x: isinstance(x, Embedding), model_2.layers)):
            layer.trainable = False

    print("##########################step 2: model summary###################################")
    model_2.summary()
    model_2.compile(tf.keras.optimizers.Adam(learning_rate=learning_rate),
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])
    print("##########################step 2: start model training#############################")
    _ = model_2.fit(datasets[TRAIN],
                    epochs=epochs,
                    steps_per_epoch=steps_per_epoch,
                    verbose=train_params['training_verbosity'],
                    callbacks=get_callbacks(f"{output_defaults['log_path']}{output_defaults['model_output_step_2']}",
                                            train_params['profile_batches'], train_params['early_stopping_patience'],
                                            checkpoint_base_path_2),
                    validation_data=datasets[VAL],
                    validation_steps=None,
                    validation_batch_size=train_params['eval_batch_size'])
    print("##########################step 2: end of model training, saving model##############")
    model_2.save(model_tag_2)

    # step 3
    # unfreezing the DNN only model trained, fine tune the model to improve performance
    # in this step, a smaller learning rate of 1/10 of the original will be used
    # unfreeze the DNN only model
    model_2.trainable = True
    print("##########################step 3: model summaries###################################")
    model_2.summary()
    model_neo_a.summary()
    model_neo_b.summary()
    model_2.compile(tf.keras.optimizers.Adam(learning_rate=learning_rate / 10),  # much lower learning rate
                    "binary_crossentropy",
                    # when incremental training we need to be careful about how auc gets instantiated
                    metrics=['binary_crossentropy', auc])

    print("##########################step 3: start model training#############################")
    history_3 = model_2.fit(datasets[TRAIN],
                            epochs=epochs,
                            steps_per_epoch=steps_per_epoch,
                            verbose=train_params['training_verbosity'],
                            callbacks=get_callbacks(f"{output_defaults['log_path']}"
                                                    f"{output_defaults['model_output_step_3']}",
                                                    train_params['profile_batches'],
                                                    train_params['early_stopping_patience'],
                                                    checkpoint_base_path_3),
                            validation_data=datasets[VAL],
                            validation_steps=None,
                            validation_batch_size=train_params['eval_batch_size'])
    print("##########################step 3: end of model training, saving model###############")
    model_2.save(model_tag_3_full)
    model_neo_a.save(model_tag_3_neo_a)
    model_neo_b.save(model_tag_3_neo_b)
    # save the weight of the prediction layer of the final tuned model
    # this serves as the bias term added to the product of Neo outputs, which altogether will feed to the sigmoid
    # function for the final prediction of probability
    bias_term = model_2.layers[-1].get_weights()[0][0]
    with open(bias_tag, 'w') as f:
        f.write(str(bias_term))
    # save to s3
    # push logs if needed
    if output_defaults['push_training_logs']:
        print(f"##########################writing logs to {base_s3_path}...#######################")
        s3_copy(f"{output_defaults['log_path']}{output_defaults['model_output_step_3']}", path_log_1,
                dryrun=FLAGS.dummy)
        s3_copy(f"{output_defaults['log_path']}{output_defaults['model_output_step_2']}", path_log_2,
                dryrun=FLAGS.dummy)
        s3_copy(f"{output_defaults['log_path']}{output_defaults['model_output_step_3']}", path_log_3,
                dryrun=FLAGS.dummy)

    # copy model outputs to final location
    print(f"##########################writing models to {base_s3_path}...######################")
    print(f"Writing step 1 combined model to {path_combined_1}...")
    s3_copy(f"{model_tag_1}", path_combined_1, dryrun=FLAGS.dummy)
    print(f"Writing step 2 combined model to {path_combined_2}...")
    s3_copy(f"{model_tag_2}", path_combined_2, dryrun=FLAGS.dummy)
    print(f"Writing step 3 combined model to {path_combined_3}...")
    s3_copy(f"{model_tag_3_full}", path_combined_3, dryrun=FLAGS.dummy)
    print(f"Writing Neo AdGroup model to {path_neo_a}...")
    s3_copy(f"{model_tag_3_neo_a}", path_neo_a, dryrun=FLAGS.dummy)
    print(f"Writing Neo BidRequest model to {path_neo_b}...")
    s3_copy(f"{model_tag_3_neo_b}", path_neo_b, dryrun=FLAGS.dummy)
    # the bias file should go in the model_files for the adgroup model instead of in a different bias folder
    print(f"Writing bias term to {path_neo_a}...")
    s3_copy(f"{output_defaults['output_path']}model/step_3/bias", path_neo_a, dryrun=FLAGS.dummy)
    # features.json should go to both adgroup and bidrequest models
    print(f"Writing features.json to {path_featuresjson_a}")
    s3_copy(f"{FEATURES_PATH}", path_featuresjson_a, recursive=False, dryrun=FLAGS.dummy)
    print(f"Writing features.json to {path_featuresjson_b}")
    s3_copy(f"{FEATURES_PATH}", path_featuresjson_b, recursive=False, dryrun=FLAGS.dummy)
    print(f"Writing model and train config file to {path_config}")
    s3_copy(output_defaults['config_save_path'], path_config, recursive=False, dryrun=FLAGS.dummy)
    print("Writing _SUCCESS files")
    s3_write_success_file(path_combined_1, dryrun=FLAGS.dummy)
    s3_write_success_file(path_combined_2, dryrun=FLAGS.dummy)
    s3_write_success_file(path_combined_3, dryrun=FLAGS.dummy)
    s3_write_success_file(path_neo_a, dryrun=FLAGS.dummy)
    s3_write_success_file(path_neo_b, dryrun=FLAGS.dummy)

    print("##########################logging model metrics########################")
    epoch_gauge.labels(region=FLAGS.region).set(epochs)
    loss_gauge.labels(region=FLAGS.region, model="philo").set(history_1.history['loss'][-1])
    loss_gauge.labels(region=FLAGS.region, model="neo").set(history_3.history['loss'][-1])
    val_loss_gauge.labels(region=FLAGS.region, model="philo").set(history_1.history['val_loss'][-1])
    val_loss_gauge.labels(region=FLAGS.region, model="neo").set(history_3.history['val_loss'][-1])

    print("##########################evaluating model on TEST########################")
    evals_1 = model_1.evaluate(datasets[TEST], verbose=2)
    evals_3 = model_2.evaluate(datasets[TEST], verbose=2)
    eval_philo_gauge.labels(region=FLAGS.region, model=model_name).set(evals_1[2])
    eval_philo_gauge.labels(region=FLAGS.region, model="neo").set(evals_3[2])

    # push metrics before stopping the job
    if not FLAGS.dummy:
        prom.push()


if __name__ == '__main__':
    app.run(main)
