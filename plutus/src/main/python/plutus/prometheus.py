from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


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

