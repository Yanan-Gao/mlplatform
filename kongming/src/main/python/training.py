from absl import app, flags
from kongming.features import default_model_features, extended_features, default_model_dim_group, default_model_targets, get_target_cat_map, Feature
from kongming.data import cache_prefetch_dataset, tfrecord_dataset, tfrecord_parser, csv_dataset
from kongming.utils import parse_input_files, s3_copy
from kongming.models import dot_product_model, load_pretrained_embedding, auto_encoder_model
from kongming.losses import AELoss
from kongming.metrics import get_auc_dist
from kongming.prometheus import Prometheus
from tensorflow_addons.losses import SigmoidFocalCrossEntropy
import tensorflow as tf
from tensorflow.python.data.ops.dataset_ops import DatasetV2
from datetime import datetime

import atexit
import math
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sn
import os
import random
import numpy as np

# setting up training configuration
FLAGS = flags.FLAGS
# DEFAULTS
OUTPUT_PATH = "./output/"
MODEL_LOGS = "./logs/"
ASSET_PATH = "./assets_string/"
EMBEDDING_PATH = "./embedding/"

MIN_HASH_CARDINALITY = 301
MAX_CARDINALITY = 10001


# path
flags.DEFINE_string('assets_path', default=ASSET_PATH, help='asset file location.')
flags.DEFINE_string('output_path', default=OUTPUT_PATH, help='output file location for saved models.')
flags.DEFINE_string('log_path', default=MODEL_LOGS, help='log file location for model training.')
flags.DEFINE_string('log_tag', default=f"{datetime.now().strftime('%Y-%m-%d-%H')}", help='log tag')
flags.DEFINE_string('model_creation_date',
                    default=datetime.now().strftime("%Y%m%d"),
                    help='Time the model was created. Its ISO date format YYYYMMDDHH (e.g. 2021123114) defaults now'
                         'Not related to the date on the training data, rather is used to identify when the model was '
                         'trained. Primary use is for model loader to load latest model to prod.')

# Model Choices
flags.DEFINE_string('activation', default='relu', help='activation to use in the MLP')
flags.DEFINE_list("string_features", default=[], help="String features for vocab lookup")
flags.DEFINE_list("int_features", default=[], help="int features for vocab lookup")
flags.DEFINE_string('sample_weight_col', default='Weight', help='sample weight column specification')
flags.DEFINE_list("extended_features", default=[], help="List of extended features")

flags.DEFINE_string('model_choice', default="basic",
                    help=f'Model under consideration.')

flags.DEFINE_string('external_embedding_path', default=EMBEDDING_PATH,
                    help=f'Embedding to take in.')
flags.DEFINE_list("top_mlp_units", default=[256, 64], help="Number of units per layer in top MLP of DLRM")
flags.DEFINE_list("ae_units", default=[1024, 512, 128, 512, 1024], help="Number of units per layer in AE")
flags.DEFINE_integer("ae_embedding_sequence", default=4, help="Hidden encoding layer id.")
flags.DEFINE_string("embedding_model_name", default=None, help="Name of the trained embedding model.")


flags.DEFINE_float('dropout_rate', default=0.3, help='Batch size for evaluation')
flags.DEFINE_boolean('batchnorm', default=True, help='Use BatchNormalization or not.')

#loss function
flags.DEFINE_string("loss_func", default='ce', help="Loss function to choose.")

# Train params
flags.DEFINE_integer('batch_size', default=4096*2*8, help='Batch size for training', lower_bound=1)
flags.DEFINE_integer('num_epochs', default=1, help='Number of epochs for training', lower_bound=1)
flags.DEFINE_float('learning_rate', default=0.0028, help='Model learning rate', lower_bound=0.001)
flags.DEFINE_boolean('use_csv', default=False, help='Whether to use csv or not (tfrecord by default)')

# Eval params
flags.DEFINE_integer('eval_batch_size', default=4096*2, help='Batch size for evaluation')

# Logging and Metrics
flags.DEFINE_boolean('push_training_logs', default=False, help=f'option to push all logs to s3 for debugging, defaults to false')
flags.DEFINE_boolean('push_metrics', default=False, help='To push prometheus metrics or not')
flags.DEFINE_boolean('generate_adgroup_auc', default=False, help='Whether to generate and save adgroup AUC')

# Call back
flags.DEFINE_integer('early_stopping_patience', default=5, help='patience for early stopping', lower_bound=2)
flags.DEFINE_list("profile_batches", default=[100, 120], help="batches to profile")


#TODO: will need to move these functions to some abstract class or within the models.py file.
def get_features_dim_target():
    extended_model_features = [f for k in FLAGS.extended_features if k in extended_features.keys()
                              for f in extended_features[k] if len(extended_features[k]) > 0]
    model_features = default_model_features + extended_model_features

    features = [f._replace(ppmethod='string_vocab')._replace(type=tf.string)._replace(default_value='UNK')
                if f.name in FLAGS.string_features else f for f in model_features]

    if default_model_dim_group.name in FLAGS.string_features:
        model_dim = default_model_dim_group._replace(ppmethod='string_mapping')._replace(type=tf.string)._replace(
            default_value='UNK')
    elif default_model_dim_group.name in FLAGS.int_features:
        model_dim = default_model_dim_group._replace(ppmethod='int_mapping')._replace(type=tf.int64)._replace(
            default_value=0)
    else:
        model_dim = default_model_dim_group

    if FLAGS.model_choice != 'ae':
        targets = default_model_targets
    else:
        # ae model feature, target setup
        targets = []
        for f in features:
            if f.type == tf.string and f.cardinality < MIN_HASH_CARDINALITY:
                targets.append(f._replace(cardinality=f.cardinality * 2))
            elif f.type == tf.int64 and f.cardinality > MAX_CARDINALITY:
                targets.append(f._replace(cardinality=MAX_CARDINALITY))
            else:
                targets.append(f)
    return features, model_dim, targets


def get_csv_cols(features, dim_features, targets, sw_col):
    selected_cols = {f.name: f for f in features}
    selected_cols.update({d.name: d for d in dim_features})
    selected_cols.update({t.name: t for t in targets})

    if sw_col != None:
        selected_cols[sw_col] = Feature(sw_col, tf.float32, cardinality=None, default_value=None)

    return selected_cols

def get_data(features, dim_features, targets, sw_col, num_gpus, use_csv = False):
    #function to return
    train_files, val_files = parse_input_files(FLAGS.input_path+"train/"), parse_input_files(FLAGS.input_path+"val/")

    if len(train_files) == 0 or len(val_files) == 0:
        raise Exception("No training or validation files")

    if (use_csv):
        selected_cols = get_csv_cols(features, dim_features, targets, sw_col)

        # TODO: change this to use targets
        label_name = "Target"
        train = csv_dataset(train_files, FLAGS.batch_size * num_gpus, selected_cols, label_name, sw_col)
        val = csv_dataset(val_files, FLAGS.eval_batch_size * num_gpus, selected_cols, label_name, sw_col)
    else:
        map_fn = tfrecord_parser(features, dim_features, targets, FLAGS.model_choice, MAX_CARDINALITY, sw_col)
        train = tfrecord_dataset(train_files, FLAGS.batch_size * num_gpus, map_fn)
        val = tfrecord_dataset(val_files, FLAGS.eval_batch_size * num_gpus, map_fn)
    return train, val


def get_model(features, dim_feature, assets_path, target_size):
    # function for building the model
    if FLAGS.model_choice == "basic":
        model = dot_product_model(features,
                          dim_feature,
                          activation="relu",
                          top_mlp_layers=FLAGS.top_mlp_units,
                          dropout_rate=FLAGS.dropout_rate,
                          batchnorm=FLAGS.batchnorm,
                          assets_path=assets_path
                          )
    elif FLAGS.model_choice == "ae":
        model = auto_encoder_model(features,
                          ae_units=FLAGS.ae_units,
                          dropout_rate=FLAGS.dropout_rate,
                          batchnorm=FLAGS.batchnorm,
                          assets_path=assets_path,
                          target_size=target_size
                          )
    elif FLAGS.model_choice == "basic-ae":
        encoder=load_pretrained_embedding(path=FLAGS.external_embedding_path+'model/'+FLAGS.embedding_model_name, layer_id=FLAGS.ae_embedding_sequence)
        model = dot_product_model(features,
                          dim_feature,
                          activation="relu",
                          top_mlp_layers=FLAGS.top_mlp_units,
                          dropout_rate=FLAGS.dropout_rate,
                          batchnorm=FLAGS.batchnorm,
                          assets_path=assets_path,
                          pretrained_layer=encoder
                          )
    else:
        raise Exception("unknown model type.")
    return model

def get_loss(cat_dict):
    if FLAGS.model_choice=='ae':
        loss = {'cat':AELoss(cat_dict), 'cont':'mse'}
        return loss
    else:
        if FLAGS.loss_func=='ce':
            return tf.keras.losses.BinaryCrossentropy()
        elif FLAGS.loss_func=='fce':
            return SigmoidFocalCrossEntropy()
        else:
            raise Exception("Loss not supported.")

def get_metrics():
    if FLAGS.model_choice == 'ae':
        return ['accuracy']
    else:
        return [tf.keras.metrics.AUC()]

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

def get_ae_target_size(dataset):
    # this dataset has one one sample
    if FLAGS.model_choice=='ae':
        for examples in dataset:
            cat_size, cont_size = examples[1]['cat'].shape[1], examples[1]['cont'].shape[1]
            break
        return {'cat':cat_size, 'cont':cont_size}
    else:
        return None

def set_adgroup_auc(adgroup_auc_gauge, adgroupid, train_auc, val_auc):
    adgroup_auc_gauge.labels(adgroupid, 'train').set(train_auc)
    adgroup_auc_gauge.labels(adgroupid, 'val').set(val_auc)

def push_metrics(history, auc_df: pd.DataFrame):
    prometheus = Prometheus(environment=FLAGS.env, job_name="Kongming", application="UncalibratedTraining")

    #todo: find out to get these with early stopping
    epoch_gauge = prometheus.define_gauge('epochs', 'number of epochs')
    epoch_gauge.set(len(history.epoch))
    # steps_gauge = prometheus.define_gauge('num_steps', 'number of steps per epoch')

    loss_gauge = prometheus.define_gauge('loss', 'loss value')
    auc_gauge = prometheus.define_gauge('auc', 'auc value')
    val_loss_gauge = prometheus.define_gauge('val_loss', 'validation loss')
    val_auc_gauge = prometheus.define_gauge('val_auc', 'validation auc')

    loss_gauge.set(history.history['loss'][-1])
    auc_gauge.set(history.history['auc'][-1])
    val_loss_gauge.set(history.history['val_loss'][-1])
    val_auc_gauge.set(history.history['val_auc'][-1])

    if auc_df is not None:
        adgroup_auc_gauge = prometheus.define_gauge('adgroup_auc', 'AUC during training', ['adgroupid', 'auc_type'])

        auc_df.fillna(0).apply(lambda row:
            set_adgroup_auc(adgroup_auc_gauge, row['AdGroupId'], row['train_auc'], row['val_auc']), axis=1
        )

    prometheus.push()

def generate_adgroup_auc(model: tf.keras.Model, train: DatasetV2, val: DatasetV2) -> pd.DataFrame:
    # upload train_auc and val_auc to S3
    train_auc = get_auc_dist(train, model, "train_auc", "train_count")
    val_auc = get_auc_dist(val, model, "val_auc", "val_count")
    combined_auc = pd.merge(train_auc, val_auc, on='AdGroupId', how='outer')

    auc_path = "./auc/"
    os.makedirs(auc_path, exist_ok=True)

    auc_result_location = f"{auc_path}auc.csv"
    combined_auc.to_csv(auc_result_location, header=True, index=False)

    s3_auc_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{FLAGS.env}/kongming/measurement/auc/v=1/date={FLAGS.model_creation_date}"
    s3_copy(auc_path, s3_auc_output_path)

    # generate plot and upload to s3
    auc_plot_path = "./auc_plot/"
    os.makedirs(auc_plot_path, exist_ok=True)

    sn.histplot(train_auc.train_auc)
    sn.histplot(val_auc.val_auc)
    plt.savefig(f"{auc_plot_path}auc.png")

    s3_auc_plot_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{FLAGS.env}/kongming/measurement/auc_plot/v=1/date={FLAGS.model_creation_date}"
    s3_copy(auc_plot_path, s3_auc_plot_path)

    return combined_auc

def set_seed(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)
    tf.experimental.numpy.random.seed(seed)
    # When running on the CuDNN backend, two further options must be set
    os.environ['TF_CUDNN_DETERMINISTIC'] = '1'
    os.environ['TF_DETERMINISTIC_OPS'] = '1'
    # Set a fixed value for the hash seed
    os.environ["PYTHONHASHSEED"] = str(seed)
    print(f"Random seed set as {seed}")

def main(argv):
    set_seed(FLAGS.seed)

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    num_gpus = strategy.num_replicas_in_sync
    print('Number of devices: {}'.format(num_gpus))

    model_features, model_dim_feature, model_targets = get_features_dim_target()

    train, val = get_data(model_features,
                          [model_dim_feature],
                          model_targets,
                          FLAGS.sample_weight_col,
                          num_gpus,
                          use_csv=FLAGS.use_csv)

    target_size = get_ae_target_size(train.take(1))
    train = cache_prefetch_dataset(train)
    val = cache_prefetch_dataset(val)

    cat_dict = get_target_cat_map(model_targets, MAX_CARDINALITY) if FLAGS.model_choice == 'ae' else None

    with strategy.scope():
        model = get_model(model_features, model_dim_feature, FLAGS.assets_path, target_size)

        # todo: do we need this?
        model.summary()

        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=FLAGS.learning_rate * math.sqrt(num_gpus)),
                      loss=get_loss(cat_dict),
                      metrics=get_metrics())
                      # loss_weights=None,
                      # run_eagerly=None,
                      # steps_per_execution=None)

    history = model.fit(train,
                        epochs=FLAGS.num_epochs,
                        verbose=2,
                        callbacks=get_callbacks(),
                        validation_data=val,
                        validation_batch_size=FLAGS.eval_batch_size)

    # fix the segfault issue because tensorflow doesn't correctly close the pools
    # https://github.com/tensorflow/tensorflow/issues/50487#issuecomment-1071515037
    atexit.register(strategy._extended._cross_device_ops._pool.close) # type: ignore
    atexit.register(strategy._extended._host_cross_device_ops._pool.close) #type: ignore

    output_path = FLAGS.output_path if FLAGS.model_choice!='ae' else FLAGS.external_embedding_path
    model_path = f"{output_path}model/{FLAGS.model_choice}_{FLAGS.dropout_rate}/"
    model.save(model_path)

    s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/{FLAGS.experiment}/conversion_model/date={FLAGS.model_creation_date}"
    s3_copy(model_path, s3_output_path)

    if (FLAGS.push_training_logs):
        s3_log_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/{FLAGS.experiment}/conversion_model_logs/date={FLAGS.model_creation_date}"
        s3_copy(FLAGS.log_path, s3_log_path)

    auc_df = None
    if FLAGS.generate_adgroup_auc:
        auc_df = generate_adgroup_auc(model, train, val)

    if FLAGS.push_metrics:
        push_metrics(history, auc_df)

if __name__ == '__main__':
    app.run(main)
