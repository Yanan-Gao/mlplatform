package com.thetradedesk.spark.util.prometheus

import java.util

import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.{Collector, CollectorRegistry, Counter, Gauge, GaugeMetricFamily, Histogram}
import org.apache.log4j.Logger
import com.thetradedesk.spark.util.TTDConfig
import com.thetradedesk.spark.util.TTDConfig.config

import scala.collection.JavaConversions._

class PrometheusClient(application: String, jobName: String) {
  @transient private lazy val log = Logger.getLogger(getClass.getName)
  // default address
  private val pushGateway = new PushGateway(config.getString("prometheusPushGateway", "prom-push-gateway.adsrvr.org:80"))
  // we need a java map here
  private val groupingKey: util.Map[String, String] = mapAsJavaMap(Map(
    "application" -> application,
    "environment" -> TTDConfig.environment.toString))

  private val registry = new CollectorRegistry

  def createGauge(name: String, help: String, labelNames: String*): Gauge = {
    Gauge.build
      .name(name)
      .help(help)
      .labelNames(labelNames: _*)
      .register(registry)
  }

  def createCounter(name: String, help: String, labelNames: String*): Counter = {
    Counter.build
      .name(name)
      .help(help)
      .labelNames(labelNames: _*)
      .register(registry)
  }

  def registerCounter(counter: Counter): Unit = {
    counter.register(registry)
  }

  def createHistogram(name: String, help: String, buckStart: Double, buckWidth: Double, buckCount: Int, labelNames: String*): Histogram = {
    Histogram.build
      .name(name)
      .linearBuckets(buckStart, buckWidth, buckCount)
      .help(help)
      .labelNames(labelNames: _*)
      .register(registry)
  }

  def registerHistogram(histogram: Histogram): Unit = {
    histogram.register(registry)
  }

  def pushMetrics(): Boolean = {
    // will attempt to push metrics 3 times
    var left = 3
    var success = false
    log.info("Pushing metrics to Prometheus...")

    do {
      try {
        left = left - 1
        pushGateway.pushAdd(registry, jobName, groupingKey)
        // if we got here we can claim success
        success = true
      } catch {
        case e: Exception =>
          // log and sleep before trying again
          log.error("Error pushing to Prometheus", e)
          Thread.sleep(2000)
      }
    } while (!success && left > 0)

    log.info("Done pushing metrics")
    // it's up to the caller to figure out what to do in case of failure
    success
  }

  // Copied from Identity repo neocortex code.
  // use Map to record metricFamily, like a metric family collector
  private var mfc = scala.collection.mutable.Map[String, GaugeMetricFamily]()
  // create a gauge family
  def createGaugeFamily(metricFamilyName: String, help: String, labelName: Seq[String]): Unit = {
    if (mfc.get(metricFamilyName).isEmpty)
      mfc += (metricFamilyName -> new GaugeMetricFamily(metricFamilyName, help, labelName))
  }

  // add metric to a exsiting gauge family
  def addToGaugeFamily(metricFamilyName: String, metrics: (Seq[String], Double)*): Unit = {
    val metricFamily = mfc.get(metricFamilyName).get
    metrics.map(m => metricFamily.addMetric(m._1, m._2))
  }

  def registerGauge(gauge: Gauge): Unit = {
    gauge.register(registry)
  }

  def pushGaugeFamily(): Unit = {
    val collector = new Collector {
      override def collect() = mfc.map(m => m._2).toList
    }

    try {
      pushGateway.pushAdd(collector, jobName, groupingKey)
    } catch {
      case e: Exception =>
        println(s"error of pushing gateway: ${e.getMessage}")
    }
    println("pushing gateway successfully")
  }
}
