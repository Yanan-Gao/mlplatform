import sys

from absl import app, flags

from . import training

FLAGS = flags.FLAGS

flags.DEFINE_boolean("run_train", default=False, help="Run training step")

# Shared variables
S3_MODELS = "s3://thetradedesk-mlplatform-us-east-1/models"
ENV = "prod"
INPUT_PATH = "./input"
flags.DEFINE_string('env', default=ENV, help='training environment.')
flags.DEFINE_string('input_path', default=INPUT_PATH, help=f'Location of training input files (TFRecord). Default {INPUT_PATH}')
flags.DEFINE_string('s3_models', default=S3_MODELS, help='output model s3 location.')

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)


def main(argv):
    print('Running application with configuration items: %s' % ' '.join([ f + '=' + str(getattr(FLAGS, f)) for f in FLAGS ]))

    if (FLAGS.run_train):
        print('Starting training step...')
        training.main(argv)


if __name__ == '__main__':
    app.run(main)
