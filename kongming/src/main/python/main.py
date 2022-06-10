from absl import app, flags
import training, scoring, calibration
import sys

app.define_help_flags()
app.parse_flags_with_usage(sys.argv)

def main(argv):
    print('Starting training step...')
    training.main(argv)
    print('Starting scoring step...')
    scoring.main(argv)
    print('Starting calibration step...')
    calibration.main(argv)

if __name__ == '__main__':
    app.run(main)
