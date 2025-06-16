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
import com.aerospike.client.listener.{DeleteListener, WriteListener}
import com.aerospike.client.policy._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.spark.sql.{DataFrame, Dataset}

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

  private def getCurrentFilePath() = {
    s"$MLPlatformS3Root/${ttdEnv}/seed_density_feature_meta/v=1/_CURRENT"
  }

  override def runTransform(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient("FeatureStore", "DailySeedMappingIdDensityScoreAerospikePublishingJob")
    val metricsAerospikeWriteGauge = prometheus.createGauge("perf_auto_seed_density_score_aerospike_write_result", "Aerospike write result count by status", "status")
    val metricsAerospikeDeleteGauge = prometheus.createGauge("perf_auto_seed_density_score_aerospike_delete_result", "Aerospike delete result count by status", "status")


    // spark configuration
    spark.conf.set("spark.sql.orc.impl", "native")

    // get configurations
    val ttl = config.getInt("ttl", 86400 * 180)
    val aerospikeSet = config.getString("aerospikeSet", "sds")
    val aerospikeAddress = config.getStringRequired("aerospikeAddress")
    val aerospikeNamespace = config.getString("aerospikeNamespace", "ttd-user")
    val aerospikeMaxConcurrencyPerTask = config.getInt("aerospikeMaxConcurrencyPerTask", 16)
    val numEventLoops = config.getInt("numEventLoops", 1)
    val dc = config.getStringRequired("aerospikeDc")
    val maxDeletionPercentage = config.getDouble("maxDeletionPercentage", 0.5)
    val deletionWindowDays = config.getInt("deletionWindowDays", 7)

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
      .cache()

    val keysToDelete = getKeysToDelete(featureVersion, deletionWindowDays, maxDeletionPercentage)

    // write today's records
    val writeResults = writeNewRecordsToAerospike(
      seedMappingIdDensityScores,
      featureVersion,
      aerospikeSeedHost,
      aerospikePort,
      ttl,
      aerospikeMaxConcurrencyPerTask,
      numEventLoops,
      aerospikeSecrets,
      aerospikeNamespace,
      aerospikeSet
    )

    var successFileContent = s"writeSuccessCount:${writeResults._1} writeFailCount:${writeResults._2}"

    // delete the previous keys that are not in today's keys
    if (keysToDelete != null) {
      val deleteResults = deleteOldRecordsFromAerospike(
        keysToDelete,
        aerospikeSeedHost,
        aerospikePort,
        ttl,
        aerospikeMaxConcurrencyPerTask,
        numEventLoops,
        aerospikeSecrets,
        aerospikeNamespace,
        aerospikeSet
      )

      metricsAerospikeDeleteGauge.labels("success").set(deleteResults._1)
      metricsAerospikeDeleteGauge.labels("fail").set(deleteResults._2)
      metricsAerospikeDeleteGauge.labels("deleted").set(deleteResults._3)

      successFileContent = s"$successFileContent deleteSuccessCount:${deleteResults._1} deleteFailCount:${deleteResults._2} deletedCount:${deleteResults._3}"
    }

    FSUtils.writeStringToFile(successFilePath, successFileContent)

    metricsAerospikeWriteGauge.labels("success").set(writeResults._1)
    metricsAerospikeWriteGauge.labels("fail").set(writeResults._2)

    prometheus.pushMetrics()
  }

  private def getKeysToDelete(
                               currentFeatureVersion: String,
                               deletionWindowDays: Int,
                               maxDeletionPercentage: Double
                             ): Dataset[String] = {
    try {
      val featureVersions = FSUtils.readStringFromFile(getCurrentFilePath())
        .split("\\r?\\n")
        .map(_.trim)
        .filter(v => v.length > 0 && v <= currentFeatureVersion)
        .sortWith(_ > _)
        .take(deletionWindowDays)

      if (featureVersions.length < deletionWindowDays) {
        Logger.log(s"not enough version in current file to run a deletion, version count: ${featureVersions.length}")
        return null
      }

      // pick to the oldest version to delete from
      val versionToScan = featureVersions.last
      val featureKeysToScan = readCBufferFormattedSeedDensity(versionToScan)
        .select($"FeatureKeyValueHashed")
        .cache()

      val featureKeysToCompareWith = featureVersions
        .filter(v => v != versionToScan)
        .map(v => readCBufferFormattedSeedDensity(v).select($"FeatureKeyValueHashed"))
        .reduce(_ union _)

      // for example: delete keys in day 1 that do not exist in day 2 to day 6
      val keysToDelete = featureKeysToScan
        .except(featureKeysToCompareWith)
        .as[String]
        .cache()

      val keysToDeleteCount = keysToDelete.count()
      val keysToScanCount = featureKeysToScan.count()

      // if too many keys will be deleted, that probably means today's data is corrupted
      if (keysToDeleteCount * 1.0 / keysToScanCount > maxDeletionPercentage) {
        throw new Exception(s"Too many keys to delete, keys to scan count: $keysToScanCount keys to delete count: $keysToDeleteCount")
      }

     keysToDelete
    }
    catch {
      case e: Exception if e.getMessage.contains("Too many keys to delete") =>
        throw e
      case e: Exception =>
        Logger.log(s"current file does not exist, Exception: ${e.toString}")
        null
    }
  }

  private def writeNewRecordsToAerospike(
                                          records: Dataset[SeedDensityFeature],
                                          featureVersion: String,
                                          aerospikeSeedHost: String,
                                          aerospikePort: Int,
                                          ttl: Int,
                                          aerospikeMaxConcurrencyPerTask: Int,
                                          numEventLoops: Int,
                                          aerospikeSecrets: AerospikeSecrets,
                                          aerospikeNamespace: String,
                                          aerospikeSet: String
                                        ): (Long, Long) = {
    records
      .map(row => {
        AerospikeSeedDensityScoreRecord(
          row.FeatureKeyValueHashed,
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

            val eventLoop = aerospikeClientFactory.eventLoops.next()
            val eventLoopIndex = eventLoop.getIndex

            if (aerospikeClientFactory.throttles.waitForSlot(eventLoopIndex, 1)) {
              try {
                aerospikeClient.put(
                  eventLoop,
                  new AsyncWriteListener(aerospikeClientFactory.throttles, eventLoopIndex, atomicCounter),
                  aerospikeClientFactory.writePolicy,
                  key,
                  bin_d, bin_v
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
  }

  private def deleteOldRecordsFromAerospike(
                                             records: Dataset[String],
                                             aerospikeSeedHost: String,
                                             aerospikePort: Int,
                                             ttl: Int,
                                             aerospikeMaxConcurrencyPerTask: Int,
                                             numEventLoops: Int,
                                             aerospikeSecrets: AerospikeSecrets,
                                             aerospikeNamespace: String,
                                             aerospikeSet: String
                                           ): (Long, Long, Long) = {
    records
      .mapPartitions(p => {
        AerospikeCredsAndSecretsUtils.install()
        p
      })
      .rdd
      .mapPartitions(
        p => {
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
          val atomicCounter = AerospikeDeleteCounters()

          p.foreach(record => {
            numRecords += 1
            val key = new Key(aerospikeNamespace, aerospikeSet, record)

            val eventLoop = aerospikeClientFactory.eventLoops.next()
            val eventLoopIndex = eventLoop.getIndex

            if (aerospikeClientFactory.throttles.waitForSlot(eventLoopIndex, 1)) {
              try {
                aerospikeClient.delete(
                  eventLoop,
                  new AsyncDeleteListener(aerospikeClientFactory.throttles, eventLoopIndex, atomicCounter),
                  aerospikeClientFactory.writePolicy,
                  key,
                )
              } catch {
                case e: Exception =>
                  Logger.log("delete exception " + e.toString)
                  atomicCounter.failCounter.incrementAndGet()
                  aerospikeClientFactory.throttles.addSlot(eventLoopIndex, 1)
              }
            } else {
              Logger.log("failed to get slot")
              atomicCounter.failCounter.incrementAndGet()
            }
          })

          while (atomicCounter.totalCount() != numRecords) {
            Logger.log("waiting for all records' deletion to complete")
            Thread.sleep(1000)
          }

          aerospikeClient.close()
          aerospikeClientFactory.close()

          Seq((atomicCounter.successCounter.get(), atomicCounter.failCounter.get(), atomicCounter.deletedCounter.get())).toIterator
        }
      )
      .toDF("SuccessCount", "FailCount", "DeletedCount")
      .agg(
        sum($"SuccessCount".as("SuccessCount")),
        sum($"FailCount".as("FailCount")),
        sum($"DeletedCount".as("DeletedCount"))
      )
      .as[(Long, Long, Long)]
      .head
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

case class AerospikeDeleteCounters(
                                    successCounter: AtomicLong = new AtomicLong(0L),
                                    failCounter: AtomicLong = new AtomicLong(0L),
                                    deletedCounter: AtomicLong = new AtomicLong(0L)
                                  ) {
  def totalCount(): Long = {
    successCounter.get() + failCounter.get()
  }
}

class AsyncDeleteListener(throttles: Throttles, eventLoopIndex: Int, counter: AerospikeDeleteCounters) extends DeleteListener {

  override def onSuccess(key: Key, existed: Boolean): Unit = {
    counter.successCounter.incrementAndGet()
    if (existed) {
      counter.deletedCounter.incrementAndGet()
    }
    throttles.addSlot(eventLoopIndex, 1)
  }

  override def onFailure(ae: AerospikeException): Unit = {
    counter.deletedCounter.incrementAndGet()
    throttles.addSlot(eventLoopIndex, 1)
    Logger.log(s"Delete failed with: ${ae.toString}")
  }
}

object Logger {
  def log(msg: String): Unit = {
    println(s"DailySeedMappingIdDensityScoreAerospikePublishingJobLogger: $msg")
  }
}