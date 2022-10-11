from absl import app, flags
from kongming.features import default_model_features, default_model_dim_group, default_model_targets, get_target_cat_map
from kongming.data import parse_input_files, tfrecord_dataset, tfrecord_parser, s3_copy
from kongming.models import dot_product_model, load_pretrained_embedding, auto_encoder_model
from kongming.losses import AELoss
from kongming.prometheus import Prometheus
from tensorflow_addons.losses import SigmoidFocalCrossEntropy
import tensorflow as tf
from datetime import datetime

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
flags.DEFINE_integer('batch_size', default=4096*2, help='Batch size for training', lower_bound=1)
flags.DEFINE_integer('num_epochs', default=1, help='Number of epochs for training', lower_bound=1)

# Logging and Metrics
flags.DEFINE_boolean('push_training_logs', default=False, help=f'option to push all logs to s3 for debugging, defaults to false')
flags.DEFINE_boolean('push_metrics', default=False, help='To push prometheus metrics or not')

# Eval params
flags.DEFINE_integer('eval_batch_size', default=10, help='Batch size for evaluation')

# Call back

flags.DEFINE_integer('early_stopping_patience', default=5, help='patience for early stopping', lower_bound=2)
flags.DEFINE_list("profile_batches", default=[100, 120], help="batches to profile")


#TODO: will need to move these functions to some abstract class or within the models.py file.
def get_features_dim_target():

    features = [f._replace(ppmethod='string_vocab')._replace(type=tf.string)._replace(default_value='UNK')
                if f.name in FLAGS.string_features else f for f in default_model_features]
    model_dim = default_model_dim_group._replace(ppmethod='string_mapping')._replace(type=tf.string)._replace(
        default_value='UNK') \
        if default_model_dim_group.name in FLAGS.string_features else default_model_dim_group

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




def get_data(features, dim_feature, targets, sw_col):
    #function to return
    train_files, val_files = parse_input_files(FLAGS.input_path+"train/"), parse_input_files(FLAGS.input_path+"val/")

    if len(train_files) == 0 or len(val_files) == 0:
        raise Exception("No training or validation files")

    train = tfrecord_dataset(train_files,
                                FLAGS.batch_size,
                                tfrecord_parser(features, dim_feature, targets, FLAGS.model_choice, MAX_CARDINALITY, sw_col)
                                )
    val = tfrecord_dataset(val_files,
                                FLAGS.eval_batch_size,
                                tfrecord_parser(features, dim_feature, targets, FLAGS.model_choice, MAX_CARDINALITY, sw_col)
                                )
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

def push_metrics(history):
    prometheus = Prometheus("KongmingTraining")

    #todo: find out to get these with early stopping
    # epoch_gauge = prometheus.define_gauge('epochs', 'number of epochs')
    # steps_gauge = prometheus.define_gauge('num_steps', 'number of steps per epoch')

    loss_gauge = prometheus.define_gauge('loss', 'loss value')
    auc_gauge = prometheus.define_gauge('auc', 'auc value')
    val_loss_gauge = prometheus.define_gauge('val_loss', 'validation loss')
    val_auc_gauge = prometheus.define_gauge('val_auc', 'validation auc')

    loss_gauge.set(history.history['loss'][0])
    auc_gauge.set(history.history['auc'][0])
    val_loss_gauge.set(history.history['val_loss'][0])
    val_auc_gauge.set(history.history['val_auc'][0])

    prometheus.push()

def main(argv):

    model_features, model_dim_feature, model_targets = get_features_dim_target()

    train, val= get_data(model_features, [model_dim_feature], model_targets, FLAGS.sample_weight_col)

    target_size = get_ae_target_size(train.take(1))

    model = get_model(model_features, model_dim_feature, FLAGS.assets_path, target_size)

    model.summary()

    cat_dict = get_target_cat_map(model_targets, MAX_CARDINALITY) if FLAGS.model_choice=='ae' else None

    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
                  loss=get_loss(cat_dict),
                  metrics=get_metrics())
                  # loss_weights=None,
                  # run_eagerly=None,
                  # steps_per_execution=None)

    history = model.fit(train,
                        epochs=FLAGS.num_epochs,
                        verbose=True,
                        callbacks=get_callbacks(),
                        validation_data=val,
                        validation_batch_size=FLAGS.eval_batch_size)

    output_path = FLAGS.output_path if FLAGS.model_choice!='ae' else FLAGS.external_embedding_path
    model_path = f"{output_path}model/{FLAGS.model_choice}_{FLAGS.dropout_rate}/"
    model.save(model_path)

    s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/conversion_model/date={FLAGS.model_creation_date}"
    s3_copy(model_path, s3_output_path)

    if (FLAGS.push_training_logs):
        s3_log_path = f"{FLAGS.s3_models}/{FLAGS.env}/kongming/conversion_model_logs/date={FLAGS.model_creation_date}"
        s3_copy(FLAGS.log_path, s3_log_path)

    if (FLAGS.push_metrics):
        push_metrics(history)


if __name__ == '__main__':
    app.run(main)
