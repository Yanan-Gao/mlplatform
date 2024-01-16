package job

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.transform.{ModelPromotionStats, ModelPromotionTransform}
import com.thetradedesk.plutus.data.{isBetter, isOkay}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.LocalParquet
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.sys.process._

object ModelPromotion extends Logger{

  private implicit val prometheus = new PrometheusClient("Plutus", "OnlineModelPromotion")
  private val savingsRateGauge = prometheus.createGauge("plutus_training_model_promotion_savingsrate", "Savings Rate for the duration of the model promotion test", labelNames = "model")

  private val winRateGauge = prometheus.createGauge("plutus_training_model_promotion_winrate", "Winrate Rate for the duration of the model promotion test", labelNames = "model")
  private val realizedSurplusNormGauge = prometheus.createGauge("plutus_training_model_promotion_realized_surplus_norm", "Sum(Realized Surplus)/Count(bids) for the duration of the model promotion test", labelNames = "model")
  private val potentialSurplusGauge = prometheus.createGauge("plutus_training_model_promotion_potential_surplus_norm", "Sum(Adjustment)/Count(bids) for the duration of the model promotion test", labelNames = "model")

  private val modelPromotionGauge = prometheus.createGauge("plutus_training_model_promotion_online", "Is the model being promoted from the online model promotion?")
  private val jobDurationGauge = prometheus.createGauge("plutus_training_model_promotion_run_time_seconds", "Job execution time in seconds")

  val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))

  // In test mode, we will setup a local spark context
  val spark = if (environment == LocalParquet) {
    SparkSession.builder().master("local[*]").getOrCreate()
  } else {
    TTDSparkContext.spark
  }

  private val trainBucket = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/train/"
  private val sourceBucket = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/stage/"
  private val destinationBucket = s"s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/"

  case class ModelPromotionResults(modelversion: String, startDateTime: LocalDateTime, endDateTime: LocalDateTime, stats: Array[ModelPromotionStats], isPromoted: Boolean, message: String)

  def deleteS3(path: String, recursive: Boolean = true) = {
    if (recursive) {
      s"aws s3 rm $path --recursive" !
    } else {
      s"aws s3 rm $path" !
    }
  }

  def copyS3(src: String, dest: String, recursive: Boolean = true) = {
    if(recursive) {
      s"aws s3 cp $src $dest --recursive --no-progress" !
    } else {
      s"aws s3 cp $src $dest --no-progress" !
    }
  }

  def findCsvFile(path: String): String = {
    val folder = new File(path)
    if (!folder.exists)
      throw new Exception(s"path ($path) doesn't exist")
    if (folder.isDirectory) {
      val csvFiles = folder.listFiles((_, name) => name.toLowerCase.endsWith(".csv"))
      if (csvFiles.length > 0) {
        if (csvFiles.length > 1) {
          println(s"Warning: Multiple CSV Files found in $path: (${csvFiles.length}).")
        }
        return csvFiles.head.getAbsolutePath
      }
      throw new Exception(s"path ($path) doesn't contain any files")
    }
    if (folder.isFile) {
      return path
    }
    throw new Exception(s"path ($path) is not a file or a directory.")
  }

  def main(args: Array[String]): Unit = {
    val endDateTime = config.getDateTimeRequired("enddate")
    val lookback = config.getIntRequired("lookback")
    val modelVersion = config.getStringRequired("modelversion")
    val outputPath = f"${trainBucket}model_promotion/${modelVersion}/"

    val promotionMetric = config.getString("promotionmetric", default="realizedsurplus")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    // This argument is parsed as a string because boolean arguments sent by the python/airflow/emr connection
    // aren't received by scala as boolean (they're sent in title case ('False') instead of 'false').
    val eval = config.getString("evaluate", "true").toBoolean


    println(s"EndDate: ${endDateTime}")
    println(s"ModelVersion: ${modelVersion}")
    println(s"promotionMetric: ${promotionMetric}")

    val simpleResults = ModelPromotionTransform.Transform(endDateTime, lookback, outputPath)

    val message: StringBuilder = new StringBuilder()

    val plutusStats = simpleResults.collectFirst({ case x: ModelPromotionStats if x.PlutusTfModel == Some("plutus") => x })
    val plutusStageStats = simpleResults.collectFirst({ case x: ModelPromotionStats if x.PlutusTfModel == Some("plutusStage") => x })

    val result = if (plutusStats.isDefined && plutusStageStats.isDefined) {
      savingsRateGauge.labels("prod").set(plutusStats.get.savingsRate)
      savingsRateGauge.labels("stage").set(plutusStageStats.get.savingsRate)

      winRateGauge.labels("prod").set(plutusStats.get.winRate)
      winRateGauge.labels("stage").set(plutusStageStats.get.winRate)

      realizedSurplusNormGauge.labels("prod").set(plutusStats.get.realizedSurplusNorm)
      realizedSurplusNormGauge.labels("stage").set(plutusStageStats.get.realizedSurplusNorm)

      potentialSurplusGauge.labels("prod").set(plutusStats.get.potentialSurplusNorm)
      potentialSurplusGauge.labels("stage").set(plutusStageStats.get.potentialSurplusNorm)

      val isModelPromoted = promotionMetric match {
        case "savingsrate" => {
          val isWinRateOk = isOkay(prodStat = plutusStats.get.winRate, stageStat = plutusStageStats.get.winRate)
          val isSavingsBetter = isBetter(prodStat = plutusStats.get.savingsRate, stageStat = plutusStageStats.get.savingsRate)

          message.append(f"IsSavingsBetter: ${isSavingsBetter} (stage: ${plutusStageStats.get.savingsRate} vs prod: ${plutusStats.get.savingsRate}) ")
          message.append(f"IsWinRateOk: ${isWinRateOk} (stage: ${plutusStageStats.get.winRate} vs prod: ${plutusStats.get.winRate}). ")
          isWinRateOk && isSavingsBetter
        }
        case "realizedsurplus" => {
          isBetter(prodStat = plutusStats.get.realizedSurplusNorm, stageStat = plutusStageStats.get.realizedSurplusNorm)
        }
        case "netimprovement" => {
          isBetter(prodStat = plutusStats.get.winRate * plutusStats.get.savingsRate, stageStat = plutusStageStats.get.winRate * plutusStageStats.get.savingsRate)
        }
      }
      if (isModelPromoted) {
        message.append("Model is promoted.")
        true
      } else {
        message.append("Model not promoted. ")
        false
      }
    } else {
      message.append("Couldn't find stats for plutus and plutusDev.")
      false
    }

    println(message)

    val metrics_filename = s"${timestamp}_online_promotion_result.json"

    val file = new File(metrics_filename)
    val pw = new PrintWriter(file)
    pw.write(ModelPromotionResults(modelVersion, endDateTime.minusHours(lookback), endDateTime, simpleResults, result, message.toString()).asJson.noSpaces)
    pw.close()

    copyS3(metrics_filename, outputPath, recursive = false)

    jobDurationGaugeTimer.setDuration()

    if (!eval) {
      if (result) {
        modelPromotionGauge.set(1)
        copyS3(s"${sourceBucket}models/${modelVersion}/", f"${destinationBucket}models/${modelVersion}/")
        copyS3(f"${sourceBucket}models_params/${modelVersion}/", f"${destinationBucket}models_params/${modelVersion}/")
        copyS3(f"${sourceBucket}model_logs/${modelVersion}/", f"${destinationBucket}model_logs/${modelVersion}/")
      } else {
        modelPromotionGauge.set(0)
        deleteS3(f"${sourceBucket}models_params/${modelVersion}/")
        deleteS3(f"${sourceBucket}model_logs/${modelVersion}/")
        deleteS3(f"${sourceBucket}models/${modelVersion}/")
      }
      prometheus.pushMetrics()
    }
  }
}
