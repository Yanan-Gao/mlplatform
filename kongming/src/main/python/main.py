from absl import app, flags
import training, scoring, calibration
import sys

FLAGS = flags.FLAGS

flags.DEFINE_boolean("run_train", default=False, help="Run training step")
flags.DEFINE_boolean("run_score", default=False, help="Run scoring step")
flags.DEFINE_boolean("run_calibration", default=False, help="Run calibration step")

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

if __name__ == '__main__':
    app.run(main)
