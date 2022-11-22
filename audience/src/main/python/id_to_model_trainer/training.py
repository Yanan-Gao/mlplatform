from datetime import datetime

from absl import app, flags
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
import tensorflow as tf

from . import data, features, models

# setting up training configuration
FLAGS = flags.FLAGS
OUTPUT_PATH = "./output/"

# path
flags.DEFINE_string("topic", default="ae_date0812_large", help="Model topic.")
flags.DEFINE_string(
    "output_path", default=OUTPUT_PATH, help="output file location for saved models."
)
flags.DEFINE_string(
    "model_creation_date",
    default=datetime.now().strftime("%Y%m%d"),
    help="Time the model was created. Its ISO date format YYYYMMDDHH (e.g. 2021123114) defaults now"
    "Not related to the date on the training data, rather is used to identify when the model was "
    "trained. Primary use is for model loader to load latest model to prod.",
)
flags.DEFINE_string(
    "model_weight_path",
    default=None,
    help="Path to existing model weights. The model will not be restored if this argument is not supplied.",
)
flags.DEFINE_boolean(
    "upload_weights", default=False, help="Upload weights for saved model."
)

# Model Choices
flags.DEFINE_integer(
    "search_embedding_size",
    default=64,
    help="Embedding size for impression and pixel embedding. Should match the length defined in the similarity search engine",
)

# Train params
flags.DEFINE_integer(
    "batch_size", default=10240, help="Batch size for training", lower_bound=1
)
flags.DEFINE_integer(
    "num_epochs", default=12, help="Number of epochs for training", lower_bound=1
)
flags.DEFINE_integer(
    "buffer_size", default=1000000, help="Buffer size for training", lower_bound=1
)
flags.DEFINE_string(
    "initializer", default="he_normal", help="initializer to use in the MLP"
)
flags.DEFINE_float(
    "feature_dim_factor", default=1.3, help="Tabnet feature dimension factor"
)
flags.DEFINE_integer("num_decision_steps", default=5, help="Tabnet num decision steps")

# Eval params
flags.DEFINE_integer(
    "eval_batch_size", default=102400, help="Batch size for evaluation"
)

# Call back
flags.DEFINE_boolean(
    "save_best",
    default=False,
    help="if save best is set to false, the model will exit early when the weights have not been changed for `early_stopping_patient` epoches.",
)
flags.DEFINE_integer(
    "early_stopping_patience",
    default=5,
    help="patience for early stopping",
    lower_bound=2,
)


def get_data(parser):
    train_files = data.get_tfrecord_files([FLAGS.input_path + "/train/"])
    val_files = data.get_tfrecord_files([FLAGS.input_path + "/validation/"])

    if len(train_files) == 0 or len(val_files) == 0:
        raise Exception("No training or validation files")

    train = data.tfrecord_dataset(
        train_files,
        FLAGS.batch_size,
        parser.parser(),
        train=True,
        buffer_size=FLAGS.buffer_size,
    )
    val = data.tfrecord_dataset(
        val_files, FLAGS.eval_batch_size, parser.parser(), train=False
    )

    return train, val


def get_callbacks(save_best=True):
    es = EarlyStopping(
        monitor="val_loss",
        mode="min",
        verbose=1,
        patience=FLAGS.early_stopping_patience,
        restore_best_weights=True,
    )

    checkpoint_base_path = f"{FLAGS.output_path}{FLAGS.topic}_checkpoints/"
    checkpoint_filepath = checkpoint_base_path + "weights.{epoch:02d}-{val_loss:.2f}"
    mc = ModelCheckpoint(
        checkpoint_filepath,
        save_weights_only=True,
        monitor="val_loss",
        mode="min",
        save_best_only=FLAGS.save_best,
    )
    if save_best:
        return [es, mc]
    else:
        return [es]


def main(argv):
    tf_parser = data.feature_parser(
        features.model_features + features.model_dim_group,
        features.model_targets,
        features.TargetingDataIdList,
        features.Target,
        exp_var=False,
    )

    train, val = get_data(tf_parser)

    model = models.init_model(
        features.model_features,
        features.model_dim_group,
        FLAGS.search_embedding_size,
        FLAGS.feature_dim_factor,
        FLAGS.num_decision_steps,
    )

    model.compile(
        optimizer="Nadam",
        loss=tf.keras.losses.BinaryCrossentropy(
            from_logits=False, label_smoothing=0.001
        ),
        metrics=[tf.keras.metrics.AUC()],
    )

    if FLAGS.model_weight_path is not None:
        model.load_weights(FLAGS.model_weight_path)

    # training the model
    model.fit(
        train,
        epochs=FLAGS.num_epochs,
        validation_data=val,
        callbacks=get_callbacks(save_best=FLAGS.save_best),
        verbose=True,
    )

    model_path = f"{FLAGS.output_path}model/{FLAGS.topic}/"
    model.save(model_path)

    if FLAGS.upload_weights:
        s3_output_path = f"{FLAGS.s3_models}/{FLAGS.env}/audience/model/date={FLAGS.model_creation_date}"
        data.s3_copy(model_path, s3_output_path)


if __name__ == "__main__":
    app.run(main)
