from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from typing import Iterable
import tensorflow as tf


class Prometheus:
    def __init__(self, environment: str, job_name: str, application: str):
        self.registry = CollectorRegistry()
        self.jobName = job_name
        self.pushgate = 'prom-push-gateway.adsrvr.org:80'
        self.enabled = True
        self.grouping_key = {
            "environment": environment,
            "application": application
        }

    def define_gauge(self,
                     metric_name: str,
                     metric_descripton: str,
                     label_names: Iterable[str] = ()):
        g = Gauge(name=metric_name, documentation=metric_descripton, labelnames=label_names, registry=self.registry)
        return g

    def push(self):
        if (self.enabled):
            push_to_gateway(self.pushgate, job=self.jobName, registry=self.registry, grouping_key=self.grouping_key)


class BatchTensorBoard(tf.keras.callbacks.TensorBoard):

    def on_train_batch_end(self, batch, logs=None):
        super().on_train_batch_end(batch, logs)
        self._log_batch_metrics(batch, logs)

    def _log_batch_metrics(self, batch, logs):
        """Writes batch metrics out as scalar summaries.
        Args:
            batch: Int. The global step to use for TensorBoard.
            logs: Dict. Keys are scalar summary names, values are scalars.
        """
        if not logs:
            return

        train_logs = {k: v for k, v in logs.items() if not k.startswith('val_')}
        val_logs = {k: v for k, v in logs.items() if k.startswith('val_')}
        train_logs = self._collect_learning_rate(train_logs)
        if self.write_steps_per_second:
            train_logs['steps_per_second'] = self._compute_steps_per_second()

        should_record = lambda: tf.equal(batch % self.update_freq, 0)

        with tf.summary.record_if(should_record):
            if train_logs:
                with self._train_writer.as_default():
                    for name, value in train_logs.items():
                        tf.summary.scalar('batch_' + name, value, step=epoch)
            if val_logs:
                with self._val_writer.as_default():
                    for name, value in val_logs.items():
                        name = name[4:]  # Remove 'val_' prefix.
                        tf.summary.scalar('batch_' + name, value, step=epoch)
