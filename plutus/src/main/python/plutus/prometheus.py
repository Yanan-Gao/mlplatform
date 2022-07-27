from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import tensorflow as tf


class Prometheus:

    registry = CollectorRegistry()
    jobName = 'plutusModelTraining'
    pushgate = 'prom-push-gateway.adsrvr.org:80'
    enabled = False
    ## g = Gauge('some_sort_of_counter', 'count of stuff', registry=registry)
    ## g.set(count)

    @classmethod
    def define_gauge(cls, metricName, metricDescripton):
        g = Gauge(name=metricName, documentation=metricDescripton, registry=cls.registry)
        return g

    @classmethod
    def push(cls):
        if (cls.enabled):
            push_to_gateway(cls.pushgate, job=cls.jobName, registry=cls.registry)


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
