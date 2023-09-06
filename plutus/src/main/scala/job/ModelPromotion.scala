package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.{dateRange, explicitDateTimePart, isBetter, isOkay}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{Duration, LocalDateTime, ZoneId, ZonedDateTime}
import scala.sys.process._

object ModelPromotion extends Logger{

  private implicit val prometheus = new PrometheusClient("Plutus", "OnlineModelPromotion")
  private val savingsRateGauge = prometheus.createGauge("plutus_training_model_promotion_savingsrate", "Savings Rate for the duration of the model promotion test", labelNames = "model")

  private val winRateGauge = prometheus.createGauge("plutus_training_model_promotion_winrate", "Winrate Rate for the duration of the model promotion test", labelNames = "model")

  private val modelPromotionGauge = prometheus.createGauge("plutus_training_model_promotion_online", "Is the model being promoted from the online model promotion?")

  // Used for testing
  private val env = "prod"

  // This checks if we're running in test vs prod
  def isTestEnv() = env != "prod" && env != "prodtest"

  // In test mode, we will setup a local spark context
  val spark = if (isTestEnv()) {
    SparkSession.builder().master("local[*]").getOrCreate()
  } else {
    TTDSparkContext.spark
  }

  import spark.implicits._

  private val trainBucket = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/train/"
  private val sourceBucket = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/stage/"
  private val destinationBucket = s"s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/${env}/"

  case class ModelPromotionStats(PlutusTfModel: Option[String], totalBids: Long, totalWins: Long, totalSavings: Double, totalSpend: Double) {
    val savingsRate = totalSavings / totalSpend.toDouble
    val winRate = totalWins.toDouble / totalBids
  }

  case class ModelPromotionResults(modelversion: String, startDateTime: ZonedDateTime, endDateTime: ZonedDateTime, paths: Seq[String], stats: Array[ModelPromotionStats], isPromoted: Boolean, message: String)

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

  def main(args: Array[String]): Unit = {
    val endDateTime = config.getDateTimeRequired("enddate").atZone(ZoneId.of("UTC"))
    val lookback = config.getIntRequired("lookback")
    val modelVersion = config.getStringRequired("modelversion")
    val eval = config.getBoolean("evaluate", true)

    val startDateTime = endDateTime.minusHours(lookback)

    println(s"StartDate: ${startDateTime}")
    println(s"EndDate: ${endDateTime}")
    println(s"ModelVersion: ${modelVersion}")

    val datapath = if (isTestEnv()) {
      "localdata/"
    } else {
      BidsImpressions.BIDSIMPRESSIONSS3
    }

    val paths = dateRange(startDateTime.truncatedTo(ChronoUnit.HOURS), endDateTime, Duration.ofHours(1)).map { i =>
      f"${datapath}prod/bidsimpressions/${explicitDateTimePart(i)}"
    }.toSeq;

    println("Paths:")
    paths.foreach(println(_))

    val res = spark.read.parquet(paths: _*)
      .where($"LogEntryTime" >= Timestamp.from(startDateTime.toInstant) and $"LogEntryTime" <= Timestamp.from(endDateTime.toInstant))
      .withColumn("savings",
        when($"isImp" === false, 0.0)
          .when($"ImpressionsFirstPriceAdjustment".isNull, 0.0)
          .otherwise($"SubmittedBidAmountInUSD" * (lit(1.0) - $"ImpressionsFirstPriceAdjustment"))
      )
      .withColumn("spend",
        when($"isImp" === false, 0.0)
          .when($"ImpressionsFirstPriceAdjustment".isNull, 0.0)
          .otherwise($"SubmittedBidAmountInUSD")
      )
      .groupBy("PlutusTfModel")
      .agg(
        count("*").as("totalBids"),
        sum(col("isimp").cast("long")).as("totalWins"),
        sum("savings").as("totalSavings"),
        sum("spend").as("totalSpend"),
      ).as[ModelPromotionStats].cache().collect()

    println("Results:")
    res.foreach { s =>
      println(s"PlutusTfModel=${s.PlutusTfModel}")
      println(s"totalBids=${s.totalBids}  totalWins=${s.totalWins}  totalSavings=${s.totalSavings}  totalSpend=${s.totalSpend}")
      println(s"winrate=${s.winRate}  savingsRate=${s.savingsRate}\n")
    }

    val message: StringBuilder = new StringBuilder()

    val plutusStats = res.collectFirst({ case x: ModelPromotionStats if x.PlutusTfModel == Some("plutus") => x })
    val plutusStageStats = res.collectFirst({ case x: ModelPromotionStats if x.PlutusTfModel == Some("plutusStage") => x })

    val result = if (plutusStats.isDefined && plutusStageStats.isDefined) {
      savingsRateGauge.labels("prod").set(plutusStats.get.savingsRate)
      savingsRateGauge.labels("stage").set(plutusStageStats.get.savingsRate)

      winRateGauge.labels("prod").set(plutusStats.get.winRate)
      winRateGauge.labels("stage").set(plutusStageStats.get.winRate)

      val isWinRateOk = isOkay(prodStat = plutusStats.get.winRate, stageStat = plutusStageStats.get.winRate)
      val isSavingsBetter = isBetter(prodStat = plutusStats.get.savingsRate, stageStat = plutusStageStats.get.savingsRate)

      message.append(f"IsSavingsBetter: ${isSavingsBetter} (stage: ${plutusStageStats.get.savingsRate} vs prod: ${plutusStats.get.savingsRate}) ")
      message.append(f"IsWinRateOk: ${isWinRateOk} (stage: ${plutusStageStats.get.winRate} vs prod: ${plutusStats.get.winRate}). ")
      if (isWinRateOk && isSavingsBetter) {
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

    val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val metrics_filename = s"${timestamp}_online_promotion_result.json"

    val file = new File(metrics_filename)
    val pw = new PrintWriter(file)
    pw.write(ModelPromotionResults(modelVersion, startDateTime, endDateTime, paths, res, result, message.toString()).asJson.noSpaces)
    pw.close()

    copyS3(metrics_filename, f"${trainBucket}model_promotion/${modelVersion}/", recursive = false)

    if (!eval && !isTestEnv()) {
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
    spark.close()
  }
}
