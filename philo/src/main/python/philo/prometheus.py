import atexit

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


class Prometheus:
    pushgate = 'prom-push-gateway.adsrvr.org:80'
    registry = CollectorRegistry()

    def __init__(self, job_name: str, application: str, environment: str = 'prod', enabled: bool = True):
        self._job_name = job_name
        self.grouping_key = {
            "environment": environment,
            "application": application
        }
        self._enabled = enabled
        # making sure that metrics are pushed when the process exits
        atexit.register(self.push)

    def define_gauge(self, metric_name, metric_description):
        """
        Creates a new Prometheus gauge

        Usage:
        g = define_gauge('name', 'some description')
        g.set(value)

        Args:
            metric_name: Name of the gauge
            metric_description: Extended description

        Returns:
            Gauge
        """
        g = Gauge(name=metric_name, documentation=metric_description, registry=self.registry)
        return g

    def push(self):
        if self._enabled:
            push_to_gateway(self.pushgate, job=self._job_name, registry=self.registry, grouping_key=self.grouping_key)

        # no need to run at exit if we already pushed
        atexit.unregister(self.push)
