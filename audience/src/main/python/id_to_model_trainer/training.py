from datetime import datetime
from absl import app, flags
from tensorflow.keras.callbacks import EarlyStopping
import tensorflow as tf
from . import data
from . import features
from . import models
from . import lion_optimizer
from . import utils
import random
import numpy as np
import os
import pathlib

if tf.test.is_gpu_available():
    models.GPU_setting()
# setting up training configuration
FLAGS = flags.FLAGS
TRAIN_PATH = './input/'
# MODEL_OUTPUT_PATH = "./output/"
BOARD_PATH = './tensorboard/'

# todo: log and measurement part


flags.DEFINE_string(
    "model_type", default="RSM", help="Model Type."
)

# input path
flags.DEFINE_string(
    "train_path", default=TRAIN_PATH, help="training data path"
)
flags.DEFINE_string(
    "train_prefix", default="split=Train", help="training data folder"
)
flags.DEFINE_string(
    "val_prefix", default="split=Val", help="training data folder"
)
# output path
flags.DEFINE_string(
    "model_path", default='./saved_model/', help="local address for saving the model"
)
flags.DEFINE_string(
    "model_output_path", default=None, help="the s3 for saving the cp of the local model; kind \
    like s3://{self.bucket_name}/{partition_prefix} "
)

# pretrain model path -- for incremental training
flags.DEFINE_string(
    "model_pretrained_path",
    default=None,
    help="Path to the pretrained model. The model will not be restored if this argument is not supplied.\
        Useful for the increamental training. Important: this path is a s3 address!",
)

# Train params
flags.DEFINE_integer(
    "neo_embedding_size", default=64,
    help="Set the emebdding length for the NEO input",
)
flags.DEFINE_integer(
    "batch_size", default=20480, help="Batch size for training", lower_bound=1
)
flags.DEFINE_integer(
    "num_epochs", default=30, help="Number of epochs for training", lower_bound=1
)
flags.DEFINE_integer(
    "buffer_size", default=1000000, help="Buffer size for training", lower_bound=1
)
flags.DEFINE_integer(
    "eval_batch_size", default=20480, help="Batch size for evaluation"
)
flags.DEFINE_integer(
    "num_decision_steps", default=3, help="Number decision step for the tabnet"
)
flags.DEFINE_float(
    "feature_dim_factor", default=1.3, help="Intermidiate layer size factor for the tabnet"
)
flags.DEFINE_float(
    "learning_rate", default=0.01, help="learning rate; Lion suggest 3e-4 to start, however, the lower value will have a higher \
        chance of overfitting"
)
flags.DEFINE_float(
    "beta_1", default=0.9, help="optimizer parameter for lion"
)
flags.DEFINE_float(
    "beta_2", default=0.99, help="optimizer parameter for lion"
)
flags.DEFINE_float(
    "wd", default=0.01, help="weight decay for lion"
)
flags.DEFINE_float(
    "relaxation_factor", default=3.5, help="parameter for tabnet"
)
flags.DEFINE_string(
    "optimizer", default="lion", help="available options: lion, nadam and adam"
)
flags.DEFINE_float(
    "label_smoothing", default=0.01, help="label smoothing setting"
)
flags.DEFINE_float(
    "tabnet_factor", default=0.18, help="facotr for the tabnet output"
)
flags.DEFINE_float(
    "embedding_factor", default=0.95, help="facotr for the bidimpression output"
)
flags.DEFINE_float(
    "dropout_rate", default=0.27, help="dropout ratio"
)
flags.DEFINE_string(
    "class_weight", default="{0: 1, 1: 2.52}", help="class weight"
)
flags.DEFINE_integer(
    "seed", default=13, help="random seed"
)
flags.DEFINE_bool(
    "fgm", default=False, help="whether to turn on fast gradient method -> add noisy to the training process which may improve the generalization capability"
)
flags.DEFINE_string(
    "fgm_layer", default=None, help="the name of layer that is going to be add noise"
)
flags.DEFINE_float(
    "fgm_epsilon", default=0.08, help="fgm epsilon"
)
flags.DEFINE_bool(
    "sum_residual_dropout", default=False, help="whether to add one more dropout layer after multiply layer"
)
flags.DEFINE_float(
    "sum_residual_dropout_rate", default=0.4, help="drop out ratio for sum_residual_dropout"
)
flags.DEFINE_integer(
    "ignore_index", default=None, help="ignore index"
)
flags.DEFINE_bool(
    "swa", default=False, help="whether to turn on Stochastic Weight Averaging -> anohter method for improving the generalization capability"
)
flags.DEFINE_integer(
    "swa_start_averaging", default=500, help="swa starting steps"
)
flags.DEFINE_integer(
    "swa_start_period", default=15, help="every swa_start_period steps run the swa"
)
flags.DEFINE_string(
    "loss", default='binary', help="loss function; currently support binary cross entropy and poly loss"
)
flags.DEFINE_float(
    "poly_epsilon", default=1.0, help="epsilon for poly loss"
)
flags.DEFINE_bool(
    "mixed_training", default=False, help="whether to turn on mix precision training mode"
)
flags.DEFINE_string(
    "initializer", default="he_normal", help="initializer to use in the MLP"
)

# Call back
# flags.DEFINE_boolean(
#     "save_best",
#     default=False,
#     help="if save best is set to false, the model will exit early when the weights have not been changed for `early_stopping_patient` epoches.",
# )
flags.DEFINE_integer(
    "early_stopping_patience",
    default=5,
    help="patience for early stopping",
    lower_bound=2,
)
flags.DEFINE_string(
    "board_path",
    default=BOARD_PATH,
    help="path for tensor board",
)


# for controlling the random seed while training the model
def set_seed(seed=13):
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)
    tf.experimental.numpy.random.seed(seed)
    # When running on the CuDNN backend, two further options must be set
    os.environ["TF_CUDNN_DETERMINISTIC"] = "1"
    os.environ["TF_DETERMINISTIC_OPS"] = "1"
    # Set a fixed value for the hash seed
    os.environ["PYTHONHASHSEED"] = str(seed)
    print(f"Random seed set as {seed}")


def get_data(parser, train_path, batch_size, eval_batch_size, buffer_size=100000, seed=13, train_prefix="split=Train/", val_prefix="split=Val/",):
    train_files = data.get_tfrecord_files([train_path + train_prefix])
    val_files = data.get_tfrecord_files([train_path + val_prefix])

    if len(train_files) == 0 or len(val_files) == 0:
        raise Exception("No training or validation files")

    train = data.tfrecord_dataset(
        train_files,
        batch_size,
        parser.parser(),
        train=True,
        buffer_size=buffer_size,
        seed=seed,
    )
    val = data.tfrecord_dataset(
        val_files, eval_batch_size, parser.parser(), train=False, seed=seed,
    )

    return train, val


def get_optimizer(learning_rate, optimizer='lion', beta_1=0.9, beta_2=0.99, wd=0.01):
    if optimizer == "nadam":
        return tf.keras.optimizers.Nadam(learning_rate=learning_rate)
    elif optimizer == "adam":
        return tf.keras.optimizers.Adam(learning_rate=learning_rate)
    elif optimizer == 'lion':
        return lion_optimizer.Lion(learning_rate=learning_rate, beta_1=beta_1, beta_2=beta_2, wd=wd)
    else:
        raise Exception("Optimizer not supported.")


def get_loss(loss, label_smoothing, poly_epsilon=1.0):
    if loss == "binary":
        return tf.keras.losses.BinaryCrossentropy(
            from_logits=False,
            label_smoothing=label_smoothing
        )
    elif loss == "poly":
        return models.poly1_cross_entropy(label_smoothing, poly_epsilon)
    else:
        raise Exception("Loss not supported.")


def get_callbacks(board_path, type='RSM', early_stopping_patience=5):
    tb_callback = tf.keras.callbacks.TensorBoard(
        log_dir=f"{board_path}{type}_{datetime.now().strftime('%Y-%m-%d-%H-%M')}",
        write_graph=True,
        profile_batch=[100, 200],
    )

    es = EarlyStopping(
        monitor="val_loss",
        mode="min",
        verbose=1,
        patience=early_stopping_patience,
        restore_best_weights=True,
    )

    return [tb_callback, es]


def main(argv):
    set_seed(FLAGS.seed)
    # whether turn on mixed training mode
    if FLAGS.mixed_training:
        policy = tf.keras.mixed_precision.Policy('mixed_float16')
        tf.keras.mixed_precision.set_global_policy(policy)

    # parse model version information
    version = FLAGS.train_path[:-1] # remove the last '/'
    version = version.split('/')[-1] # the output is like: date=20230515000000

    tf_parser = data.feature_parser(
        features.model_features + features.model_dim_group,
        features.model_targets,
        features.TargetingDataIdList,
        features.Target,
        exp_var=True,
    )

    train, val = get_data(tf_parser, FLAGS.train_path, FLAGS.batch_size, FLAGS.eval_batch_size, FLAGS.buffer_size, FLAGS.seed, FLAGS.train_prefix, FLAGS.val_prefix)

    # currently, the tfa package is going to merge into other keras packages and tensorflow, thus, comment this part
    # and wait for the further stable implementation
    # if FLAGS.swa:
    #     # general suggestions: start_averaging=n*(number steps in each epoch) and average_period=0.1 or 0.2*(number steps in each epoch)
    #     op = tfa.optimizers.SWA(get_optimizer(FLAGS.learning_rate,FLAGS.optimizer, FLAGS.beta_1, FLAGS.beta_2, FLAGS.wd),
    #                             start_averaging=FLAGS.swa_start_averaging, average_period=FLAGS.swa_average_period)
    # else:
    #     op = get_optimizer(FLAGS.learning_rate,FLAGS.optimizer, FLAGS.beta_1, FLAGS.beta_2, FLAGS.wd)

    op = get_optimizer(FLAGS.learning_rate, FLAGS.optimizer, FLAGS.beta_1, FLAGS.beta_2, FLAGS.wd)

    if FLAGS.model_pretrained_path is not None:
        # for incremental training -> we just load the model directly without train from scratch
        if FLAGS.optimizer == 'lion':
            # currently, the lion optimizer is implemented for the tf version <= 2.9, for the latest version, we need to change father class name
            model = tf.keras.models.load_model(FLAGS.model_pretrained_path, custom_objects={'Lion': lion_optimizer.Lion})
        else:
            model = tf.keras.models.load_model(FLAGS.model_pretrained_path)

    else:
        model = models.init_model(
            features.model_features,
            features.model_dim_group,
            FLAGS.neo_embedding_size,
            FLAGS.feature_dim_factor,
            FLAGS.num_decision_steps,
            FLAGS.relaxation_factor,
            FLAGS.tabnet_factor,
            FLAGS.embedding_factor,
            FLAGS.dropout_rate,
            FLAGS.seed,
            FLAGS.sum_residual_dropout,
            FLAGS.sum_residual_dropout_rate,
            FLAGS.ignore_index,
        )

    model.compile(
        optimizer=op,
        loss=get_loss(FLAGS.loss, FLAGS.label_smoothing, FLAGS.poly_epsilon),
        metrics=[tf.keras.metrics.AUC(), tf.keras.metrics.Recall(), tf.keras.metrics.Precision()],
    )

    # trun on fgm or not
    if FLAGS.fgm and FLAGS.fgm_layer:
        models.FGM_wrapper(model, FLAGS.fgm_layer, FLAGS.fgm_epsilon)

    class_weight = eval(FLAGS.class_weight)
    # training the model
    model.fit(
        train,
        epochs=FLAGS.num_epochs,
        validation_data=val,
        callbacks=get_callbacks(FLAGS.board_path, FLAGS.model_type, FLAGS.early_stopping_patience),
        class_weight=class_weight,
        verbose=True,
        # validation_freq=3,
        validation_batch_size=FLAGS.eval_batch_size,
    )

    if FLAGS.swa:
        op.assign_average_vars(model.trainable_variables)
        # to handle the batchnormalization, we need to use 1 epoch forward pass to update the weights of bn
        print('Extra epoch run for the Normalization weights update with SWA')
        model.compile(
            optimizer=get_optimizer(0),
            loss=get_loss(FLAGS.loss, FLAGS.label_smoothing),
            metrics=[tf.keras.metrics.AUC(), tf.keras.metrics.Recall(), tf.keras.metrics.Precision()],
        )
        model.fit(
            train,
            validation_data=None,
            epochs=1,
        )

    # local path for the trained model
    model_path = FLAGS.model_path + f'{FLAGS.model_type}/{version}/'
    pathlib.Path(model_path).mkdir(parents=True, exist_ok=True)
    model.save(model_path)
    if FLAGS.model_output_path:
        # cp local model to s3
        model_output_path = f'{FLAGS.model_output_path}{FLAGS.model_type}/{version}/'
        utils.s3_copy(model_path, model_output_path)


if __name__ == "__main__":
    app.run(main)
