from absl import app, flags
import training, scoring, calibration
import sys

FLAGS = flags.FLAGS

flags.DEFINE_boolean("run_train", default=False, help="Run training step")
flags.DEFINE_boolean("run_score", default=False, help="Run scoring step")
flags.DEFINE_boolean("run_calibration", default=False, help="Run calibration step")

# Shared variables
INPUT_PATH = "./input/"
S3_MODELS = "s3://thetradedesk-mlplatform-us-east-1/models"
ENV = "local"
flags.DEFINE_string('env', default=ENV, help='training environment.')
flags.DEFINE_string('s3_models', default=S3_MODELS, help='output model s3 location.')
flags.DEFINE_string('input_path', default=INPUT_PATH,
                    help=f'Location of input files (TFRecord). Default {INPUT_PATH}')

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)

def main(argv):
    if (FLAGS.run_train):
        print('Starting training step...')
        training.main(argv)
    if (FLAGS.run_score):
        print('Starting scoring step...')
        scoring.main(argv)
    if (FLAGS.run_calibration):
        print('Starting calibration step...')
        calibration.main(argv)

    print("FINISHED EXECUTION")

if __name__ == '__main__':
    app.run(main)
