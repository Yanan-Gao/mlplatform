import numpy as np
import tensorflow.keras.backend as K
from tensorflow.keras.callbacks import Callback


class BatchLearningRateScheduler(Callback):
    """Learning rate scheduler for each batch
    # Arguments
        schedule: a function that takes a batch index as input
            (integer, indexed from 0) and current learning rate
            and returns a new learning rate as output (float).
        verbose: int. 0: quiet, 1: update messages.
    """

    def __init__(self, schedule, verbose=0):
        super(BatchLearningRateScheduler, self).__init__()
        self.schedule = schedule
        self.verbose = verbose

    def on_batch_begin(self, batch, logs=None):
        if not hasattr(self.model.optimizer, 'lr'):
            raise ValueError('Optimizer must have a "lr" attribute.')
        lr = float(K.get_value(self.model.optimizer.lr))
        try:  # new API
            lr = self.schedule(batch, lr)
        except TypeError:  # old API for backward compatibility
            lr = self.schedule(batch)
        if not isinstance(lr, (float, np.float32, np.float64)):
            raise ValueError('The output of the "schedule" function '
                             'should be float.')
        K.set_value(self.model.optimizer.lr, lr)
        if self.verbose > 0:
            print('\nbatch %05d: LearningRateScheduler setting learning '
                  'rate to %s.' % (batch + 1, lr))

    def on_batch_end(self, batch, logs=None):
        logs = logs or {}
        logs['lr'] = K.get_value(self.model.optimizer.lr)


"""
Warm-up: A phase in the beginning of your neural network training where you start with a learning rate
much smaller than your "initial" learning rate and then increase it over a few iterations or epochs
until it reaches that "initial" learning rate.
"""


def warmup(model, epoch_steps=10000, lr_init=0.001, lr_min=0.0001, warm_factor=1.2):
    def warmup_schedule(batch):
        max_batch = epoch_steps * warm_factor
        step_lr_change = (lr_init - lr_min) / max_batch
        if batch <= max_batch:
            lr = lr_min + step_lr_change * batch
        else:
            lr = model.optimizer.lr
        return lr
    return warmup_schedule
