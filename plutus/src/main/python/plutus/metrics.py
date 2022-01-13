import time

import tensorflow as tf
from tensorflow.keras.utils import Progbar
from tensorflow import keras
import tensorflow_probability as tfp
import pandas as pd
import numpy as np
from collections import defaultdict
from tensorflow.python.ops import variables as tf_variables


class ANLP(keras.metrics.Metric):

    def __init__(self, name=None, dtype=None, **kwargs):
        super(ANLP, self).__init__(name, dtype, **kwargs)
        self.anlps = self.add_weight(name="anlp", initializer='zeros', aggregation=tf_variables.VariableAggregation.SUM)
        self.counts = self.add_weight(name="counts", initializer='zeros', aggregation=tf_variables.VariableAggregation.SUM)

    def update_state(self, y_true, y_pred, sample_weight=None):
        """
        mean of mean != mean!
        """
        self.anlps.assign_add(tf.reduce_sum(y_pred.log_prob(y_true)))
        self.counts.assign_add(len(y_true))

    def result(self):
        return -(self.anlps / self.counts)

    def reset_states(self):
        self.anlps.assign(0)
        self.counts.assign(0)


class MultiTargetANLP(ANLP):
  
    def update_state(self, y_true, y_pred, sample_weight=None):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)
        
        self.anlps.assign_add(tf.reduce_sum(y_pred.log_prob(_mb2w)))
        self.counts.assign_add(tf.cast(tf.size(_mb2w), dtype=tf.float32))


class MultiTargetRMSE(keras.metrics.RootMeanSquaredError):
  
    def update_state(self, y_true, y_pred, sample_weight=None):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)
        _mean = y_pred.mean()
        super(MultiTargetRMSE, self).update_state(_mb2w, _mean, sample_weight)


class MultiOutputMultiTargetANLP(ANLP):
    def update_state(self, y_true, y_pred, sample_weight=None):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)

        tf.print(y_pred[0])
        self.anlps.assign_add(tf.reduce_sum(y_pred[0].log_prob(_mb2w)))
        self.counts.assign_add(tf.cast(tf.size(_mb2w), dtype=tf.float32))


class MultiOutputMultiTargetRMSE(keras.metrics.RootMeanSquaredError):
  
    def update_state(self, y_true, y_pred, sample_weight=None):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)
        _mean = y_pred[0].mean()
        super(MultiTargetRMSE, self).update_state(_mb2w, _mean, sample_weight)


def multi_target_mse(y_true, y_pred):
    _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)
    _mean = y_pred.mean()
    return keras.metrics.mse(_mb2w, _mean)


class MultiTargetRegressionRMSE(keras.metrics.RootMeanSquaredError):

    def update_state(self, y_true, y_pred, sample_weight=None):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y_true, axis=1)
        super(MultiTargetRegressionRMSE, self).update_state(_mb2w, y_pred, sample_weight)


def calc_quantile(dist, bid, pushdown):
    """
    Qyantile function is not defined for mixtures. We need to use root finding to get the quantile for mixtures.

    """
    try:
        pd_value = dist.quantile(dist.cdf(bid) * pushdown)
    except NotImplementedError:
        desired_cdf = dist.cdf(bid) * pushdown
        res = tfp.math.find_root_chandrupatla(objective_fn=lambda x: dist.cdf(x) - desired_cdf)
        pd_value = res.estimated_root
    return pd_value


def calc_savings(model, data, targets, pushdown=0.99):
    _imp, _bid, _market, _mb2w, _floor = tf.unstack(targets, axis=1)
    preds = model(data, training=False)

    # multi-output check
    dist = preds[0] if isinstance(preds, list) else preds

    pd_value = calc_quantile(dist, _bid, pushdown)

    # Bid at least the floor
    floor_cleared_value = tf.maximum(pd_value, _floor)

    # offline analysis, therefore can only look at impressions and establish what % of them we would still win if
    # applying the pushdown. Also we have a lower bound on the floor price
    sum_savings = tf.reduce_sum(tf.where((_imp == 1.0) & (floor_cleared_value >= _mb2w), (_bid - floor_cleared_value), 0))

    sum_lost = tf.reduce_sum(tf.where((_imp == 1.0) & (floor_cleared_value < _mb2w), 1, 0))
    sum_kept = tf.reduce_sum(tf.where((_imp == 1.0) & (floor_cleared_value >= _mb2w), 1, 0))

    sum_imp = tf.reduce_sum(tf.where((_imp == 1.0), 1, 0))
    sum_bids = tf.reduce_sum(tf.where((_imp == 1.0), 0, 1))
    return sum_savings, (sum_kept / sum_imp), sum_lost, sum_kept, sum_imp, sum_bids


def calc_naive_savings(targets, pushdown=0.99):
    """
    Naive in that we just apply a relative pushdown on the bid
    """
    _imp, _bid, _market, _mb2w, _floor = tf.unstack(targets, axis=1)
    pd_value = _bid * pushdown

    # Bid at least the floor
    floor_cleared_value = tf.maximum(pd_value, _floor)

    sum_savings = tf.reduce_sum(
        tf.where((_imp == 1.0) & (floor_cleared_value >= _mb2w), (_bid - floor_cleared_value), 0))
    sum_lost = tf.reduce_sum(tf.where((_imp == 1.0) & (floor_cleared_value < _mb2w), 1, 0))
    sum_kept = tf.reduce_sum(tf.where((_imp == 1.0) & (floor_cleared_value >= _mb2w), 1, 0))

    sum_imp = tf.reduce_sum(tf.where((_imp == 1.0), 1, 0))
    sum_bids = tf.reduce_sum(tf.where((_imp == 1.0), 0, 1))
    return sum_savings, (sum_kept / sum_imp), sum_lost, sum_kept, sum_imp, sum_bids


def calc_anlp(ds, model):
    log_probs = np.array([])

    for i, (x, y) in enumerate(ds):
        _imp, _bid, _market, _mb2w, _floor = tf.unstack(y, axis=1)

        # multi-output check
        preds = model(x, training=False)

        # for mulit-head models the CPD is the first element
        dist = preds[0] if isinstance(preds, list) else preds

        lp = dist.log_prob(_mb2w).numpy()
        log_probs = np.append(log_probs, lp)

    return - (log_probs.sum() / log_probs.shape[0])


def calc_calibration(model, data, targets, target="mb2w"):
    _imp, _bid, _market, _mb2w, _floor = tf.unstack(targets, axis=1)

    if target == "mb2w":
        t = _mb2w
    elif target == "floor":
        t = _floor
    elif target == "bid":
        t = _bid
    else:
        raise (f"Unknown target: {target}")

    # multi-output check
    preds = model(data, training=False)
    dist = preds[0] if isinstance(preds, list) else preds

    probs = dist.cdf(t)
    bin_counts = tf.histogram_fixed_width_bins(probs, [0.0, 1.0], nbins=100)
    total_counts = tf.math.bincount(bin_counts, minlength=100)

    won_probs = tf.gather(probs, tf.where(tf.equal(_imp, 1.0)))
    won_bin_counts = tf.histogram_fixed_width_bins(won_probs, [0.0, 1.0], nbins=100)

    won_counts = tf.math.bincount(won_bin_counts, minlength=100)
    return won_counts.numpy(), total_counts.numpy()


def evaluate_model_saving(model, dataset, batch_size, batch_per_epoch=None):
    pb = Progbar(batch_per_epoch, stateful_metrics=["step_per_sec", "time_per_batch", "saving", "pct"])
    begin = time.time()

    push_downs = np.arange(.99, .01, -.01)

    savings = defaultdict(list)
    pcts = defaultdict(list)

    naive_savings = defaultdict(list)
    naive_pcts = defaultdict(list)

    for d, t in dataset:
        stime = time.time()

        # do processing
        for push in push_downs:
            saving, pct, _, _, _, _ = calc_savings(model, d, t, pushdown=push)
            savings[str(push)].append(saving)
            pcts[str(push)].append(pct)

            saving, pct, _, _, _, _ = calc_naive_savings(t, pushdown=push)
            naive_savings[str(push)].append(saving)
            naive_pcts[str(push)].append(pct)

        etime = time.time()
        batch_time = (etime - stime)
        step_time = float(batch_size) / batch_time

        values = [("steps_per_sec", step_time), ("time_per_batch", batch_time), ("saving", saving), ("pct", pct)]

        pb.add(1, values=values)

    end = time.time()
    print(f"Total Time: {end - begin}")

    data = []
    for push in push_downs:
        s = tf.reduce_sum(savings[str(push)]).numpy() / 1000.0
        p = tf.reduce_mean(pcts[str(push)]).numpy()

        n_s = tf.reduce_sum(naive_savings[str(push)]).numpy() / 1000.0
        n_p = tf.reduce_mean(naive_pcts[str(push)]).numpy()

        print(f"{push:.2f}\t\t{s:,.2f}\t{p:.2f}\t{n_s:,.2f}\t{n_p:.2f}")
        data.append([push, s, p, n_s, n_p])

    return pd.DataFrame(data, columns=["push", "saving", "pct", "naive_saving", "naive_pct"])


def eval_model_with_anlp(model, ds_test):
    # add new metrics and eval
    # https://stackoverflow.com/questions/44267074/adding-metrics-to-existing-model-in-keras/52098123

    model.compile(
        model.optimizer,
        metrics=[MultiTargetANLP()]
    )
    evals = model.evaluate(ds_test, verbose=1)
    return evals