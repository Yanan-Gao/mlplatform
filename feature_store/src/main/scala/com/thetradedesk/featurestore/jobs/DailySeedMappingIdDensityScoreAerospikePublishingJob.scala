package com.thetradedesk.featurestore.jobs

import com.google.common.io.Closeables
import com.thetradedesk.featurestore.datasets.SeedDensityFeature
import com.thetradedesk.featurestore.{MLPlatformS3Root, date, overrideOutput, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.aerospike.{AerospikeCredsAndSecretsUtils, AerospikeSecrets}
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions._
import com.aerospike.client._
import com.aerospike.client.async.{EventPolicy, NettyEventLoops, Throttles}
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.netty.channel.nio.NioEventLoopGroup

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.atomic.AtomicLong

object DailySeedMappingIdDensityScoreAerospikePublishingJob extends DensityFeatureBaseJob {
  override val jobName: String = "DailySeedMappingIdDensityScoreAerospikePublishingJob"

  private def readCBufferFormattedSeedDensity(version: String) = {
    spark.read.parquet(s"$MLPlatformS3Root/${ttdEnv}/seed_density_feature/v=1/${version}")
  }

  private def getSuccessFilePath(version: String, dc: String) = {
    s"$MLPlatformS3Root/${ttdEnv}/seed_density_feature/v=1/${version}/_SUCCESS_$dc"
  }

  override def runTransform(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FeatureStore", "DailySeedMappingIdDensityScoreAerospikePublishingJob")
    val metricsAerospikeWriteGauge = prometheus.createGauge("perf_auto_seed_density_score_aerospike_write_result", "Aerospike write result count by status", "status")

    // spark configuration
    spark.conf.set("spark.sql.orc.impl", "native")

    // get configurations
    val ttl = config.getInt("ttl", 86400 * 7)
    val aerospikeSet = config.getString("aerospikeSet", "seed-density-scores")
    val aerospikeAddress = config.getStringRequired("aerospikeAddress")
    val aerospikeNamespace = config.getString("aerospikeNamespace", "ttd-user")
    val aerospikeMaxConcurrencyPerTask = config.getInt("aerospikeMaxConcurrencyPerTask", 16)
    val numEventLoops = config.getInt("numEventLoops", 1)
    val dc = config.getStringRequired("aerospikeDc")

    val featureVersion = s"${getDateStr(date)}00"

    val successFilePath = getSuccessFilePath(featureVersion, dc)
    if (!overrideOutput && FSUtils.fileExists(successFilePath)(spark)) {
      println(s"Aerospike already written")
      return
    }

    // get Aerospike credentials
    val (aerospikeSeedHost, aerospikePort) = Option(aerospikeAddress)
      .getOrElse(throw new IllegalArgumentException("aerospikeAddress must be set"))
      .split(',')
      .map(_.split(':'))
      .map({ case Array(ip, port) => (ip, port.toInt) })
      .find(p => detectIfServerIsListening(p._1, p._2)) match {
      case Some((host, port)) => (host, port)
      case None => throw new RuntimeException(
        s"No Aerospike host reachable at any of [${aerospikeAddress}]"
      )
    }

    AerospikeCredsAndSecretsUtils.install()
    val aerospikeSecrets = AerospikeCredsAndSecretsUtils.configureAerospikeCredentials()

    // read source datasets
    val seedMappingIdDensityScores = readCBufferFormattedSeedDensity(featureVersion)
      .as[SeedDensityFeature]

    val writeResults = seedMappingIdDensityScores
      .map(row => {
        AerospikeSeedDensityScoreRecord(
          row.FeatureKeyValueHashed,
          ttl,
          row.data,
          featureVersion.toLong
        )
      })
      .mapPartitions(p => {
        AerospikeCredsAndSecretsUtils.install()
        p
      })
      .rdd
      .mapPartitions(
        p => {
          // create a client and factory per partition
          // because aerospikeClientFactory.eventLoops.next() is not thread safe
          val aerospikeClientFactory = AerospikeClientFactory(
            aerospikeSeedHost,
            aerospikePort,
            ttl,
            aerospikeMaxConcurrencyPerTask,
            numEventLoops,
            aerospikeSecrets
          )
          val aerospikeClient = aerospikeClientFactory.initializeAndCreateAerospikeClient()

          var numRecords = 0;
          val atomicCounter = AerospikeWriteCounters()

          p.foreach(record => {
            numRecords += 1

            val key = new Key(aerospikeNamespace, aerospikeSet, record.id)
            val bin_d = new Bin("d", record.d)
            val bin_v = new Bin("v", record.v)
            val bin_tl = new Bin("tl", record.tl)

            val eventLoop = aerospikeClientFactory.eventLoops.next()
            val eventLoopIndex = eventLoop.getIndex

            if (aerospikeClientFactory.throttles.waitForSlot(eventLoopIndex, 1)) {
              try {
                aerospikeClient.put(
                  eventLoop,
                  new AsyncWriteListener(aerospikeClientFactory.throttles, eventLoopIndex, atomicCounter),
                  aerospikeClientFactory.writePolicy,
                  key,
                  bin_d, bin_v, bin_tl
                )
              } catch {
                case e: Exception =>
                  Logger.log("put exception " + e.toString)
                  atomicCounter.failCounter.incrementAndGet()
                  aerospikeClientFactory.throttles.addSlot(eventLoopIndex, 1)
              }
            } else {
              Logger.log("failed to get slot")
              atomicCounter.failCounter.incrementAndGet()
            }
          })

          while (atomicCounter.totalCount() != numRecords) {
            Logger.log("waiting for all records to complete")
            Thread.sleep(1000)
          }

          aerospikeClient.close()
          aerospikeClientFactory.close()

          Seq((atomicCounter.successCounter.get(), atomicCounter.failCounter.get())).toIterator
        })
      .toDF("SuccessCount", "FailCount")
      .agg(
        sum($"SuccessCount".as("SuccessCount")),
        sum($"FailCount".as("FailCount"))
      )
      .as[(Long, Long)]
      .head

    FSUtils.writeStringToFile(successFilePath, s"successCount:${writeResults._1} failCount:${writeResults._2}")

    metricsAerospikeWriteGauge.labels("success").set(writeResults._1)
    metricsAerospikeWriteGauge.labels("fail").set(writeResults._2)
    prometheus.pushMetrics()
  }

  private def detectIfServerIsListening(host: String, port: Int): Boolean = {
    val s: Socket = new Socket()
    try {
      s.connect(new InetSocketAddress(host, port), 4000)
      return true
    } catch {
      case _: Throwable =>
    } finally {
      if (s != null) {
        Closeables.close(s, true)
      }
    }
    false
  }
}

// record in aerospike
case class AerospikeSeedDensityScoreRecord(
                                            id: String,
                                            tl: Int, // ttl
                                            d: Array[Byte],
                                            v: Long
                                          )

case class AerospikeClientFactory(
                                   aerospikeSeedHost: String,
                                   aerospikePort: Int,
                                   ttl: Int,
                                   maxConcurrencyPerTask: Int,
                                   numEventLoops: Int,
                                   secret: AerospikeSecrets
                                 ) {
  var writePolicy: WritePolicy = null
  var eventLoops: NettyEventLoops = null
  var throttles: Throttles = null

  def initializeAndCreateAerospikeClient(): AerospikeClient = {
    // create event loop
    val eventPolicy = new EventPolicy();
    eventPolicy.setMaxCommandsInProcess(maxConcurrencyPerTask)
    eventPolicy.setMaxCommandsInQueue(maxConcurrencyPerTask)
    val nioGroup = new NioEventLoopGroup(numEventLoops)
    eventLoops = new NettyEventLoops(eventPolicy, nioGroup)

    // create client
    val host = new Host(aerospikeSeedHost, aerospikePort)
    val policy = new ClientPolicy()

    policy.user = secret.credentials.username
    policy.password = secret.credentials.password
    policy.maxConnsPerNode = maxConcurrencyPerTask * numEventLoops
    policy.eventLoops = eventLoops

    val aerospikeClient = new AerospikeClient(policy, host)

    // create throttles
    throttles = new Throttles(numEventLoops, maxConcurrencyPerTask)

    // create write policy
    val writePolicyNew = aerospikeClient.getWritePolicyDefault
    writePolicyNew.expiration = ttl
    writePolicyNew.sendKey = false
    writePolicyNew.maxRetries = 3
    writePolicyNew.totalTimeout = 30000
    writePolicy = writePolicyNew

    aerospikeClient
  }

  def close(): Unit = {
    eventLoops.close()
  }
}

case class AerospikeWriteCounters(successCounter: AtomicLong = new AtomicLong(0L), failCounter: AtomicLong = new AtomicLong(0L)) {
  def totalCount(): Long = {
    successCounter.get() + failCounter.get()
  }
}

class AsyncWriteListener(throttles: Throttles, eventLoopIndex: Int, counter: AerospikeWriteCounters) extends WriteListener {

  override def onSuccess(key: Key): Unit = {
    counter.successCounter.incrementAndGet()
    throttles.addSlot(eventLoopIndex, 1)
  }

  override def onFailure(ae: AerospikeException): Unit = {
    counter.failCounter.incrementAndGet()
    throttles.addSlot(eventLoopIndex, 1)
    Logger.log(s"write failed with: ${ae.toString}")
  }
}

object Logger {
  def log(msg: String): Unit = {
    println(s"DailySeedMappingIdDensityScoreAerospikePublishingJobLogger: $msg")
  }
}