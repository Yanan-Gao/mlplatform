package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.{dateRange, explicitDateTimePart}
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
import java.time.{Duration, LocalDateTime, ZoneId, ZonedDateTime}
import scala.sys.process._

object ModelPromotion extends Logger{

  private implicit val prometheus = new PrometheusClient("Plutus", "OnlineModelPromotion")
  private val savingsRateGauge = prometheus.createGauge("plutus_training_model_promotion_savingsrate", "Savings Rate for the duration of the model promotion test", labelNames = "model")

  private val winRateGauge = prometheus.createGauge("plutus_training_model_promotion_winrate", "Winrate Rate for the duration of the model promotion test", labelNames = "model")

  private val modelPromotionGauge = prometheus.createGauge("plutus_training_model_promotion_online", "Is the model being promoted from the online model promotion?")
  val metrics_filename = "online_promotion_result.json"

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

  case class ModelPromotionStats(PlutusTfModel: Option[String], totalBids: Long, totalWins: Long, totalSavings: Double, totalSpend: BigDecimal) {
    val savingsRate = totalSavings / totalSpend.toDouble
    val winRate = totalWins.toDouble / totalBids
  }

  case class ModelPromotionResults(startDateTime: ZonedDateTime, endDateTime: ZonedDateTime, paths: Seq[String], stats: Array[ModelPromotionStats], result: String)

  def isEqualOrBetter(prodStat: Double, stageStat: Double, margin: Double = 0.05) = (stageStat - prodStat) / prodStat  > - margin

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

    val startDateTime = endDateTime.minusHours(lookback)

    println(s"StartDate: ${startDateTime}")
    println(s"EndDate: ${endDateTime}")
    println(s"ModelVersion: ${modelVersion}")

    val datapath = if (isTestEnv()) {
      "localdata"
    } else {
      BidsImpressions.BIDSIMPRESSIONSS3
    }

    val paths = dateRange(startDateTime, endDateTime, Duration.ofHours(1)).map { i =>
      f"${datapath}/prod/bidsimpressions/${explicitDateTimePart(i)}"
    }.toSeq;

    println("Paths:")
    paths.foreach(println(_))

    val res = spark.read.parquet(paths: _*)
        .where($"LogEntryTime" >= Timestamp.from(startDateTime.toInstant) and $"LogEntryTime" <= Timestamp.from(endDateTime.toInstant))
        .withColumn("savings",
          when($"ImpressionsFirstPriceAdjustment".isNull, 0.0)
            .when($"SubmittedBidAmountInUSD".isNull, 0.0)
            .otherwise($"SubmittedBidAmountInUSD" * (lit(1.0) - $"ImpressionsFirstPriceAdjustment"))
        )
        .groupBy("PlutusTfModel")
        .agg(
          count("*").as("totalBids"),
          sum(col("isimp").cast("long")).as("totalWins"),
          sum("savings").as("totalSavings"),
          sum("SubmittedBidAmountInUSD").as("totalSpend"),
        ).as[ModelPromotionStats].cache().collect()

    println("Results:")
    res.foreach{ s =>
      println(s"PlutusTfModel=${s.PlutusTfModel}")
      println(s"totalBids=${s.totalBids}  totalWins=${s.totalWins}  totalSavings=${s.totalSavings}  totalSpend=${s.totalSpend}")
      println(s"winrate=${s.winRate}  savingsRate=${s.savingsRate}\n")
    }

    val message: StringBuilder = new StringBuilder()

    val plutusStats = res.collectFirst({ case x:ModelPromotionStats if x.PlutusTfModel == Some("plutus") => x })
    val plutusStageStats = res.collectFirst({ case x:ModelPromotionStats if x.PlutusTfModel == Some("plutusStage") => x })

    val result = if (plutusStats.isDefined && plutusStageStats.isDefined) {
      savingsRateGauge.labels("prod").set(plutusStats.get.savingsRate)
      savingsRateGauge.labels("stage").set(plutusStageStats.get.savingsRate)

      winRateGauge.labels("prod").set(plutusStats.get.winRate)
      winRateGauge.labels("stage").set(plutusStageStats.get.winRate)

      val isWinRateOk = isEqualOrBetter(plutusStats.get.winRate, plutusStageStats.get.winRate)
      val isSavingsOk = isEqualOrBetter(plutusStats.get.savingsRate, plutusStageStats.get.savingsRate)

      val improvementRatio = (plutusStageStats.get.winRate * plutusStageStats.get.savingsRate) / (plutusStats.get.winRate  * plutusStats.get.savingsRate)

      if( isWinRateOk && isSavingsOk && improvementRatio >= 1) {
        message.append("Model is promoted.")

        true
      } else {
        modelPromotionGauge.set(0)
        message.append("Model not promoted because its not good. ")
        message.append(f"IsSavingsOk: ${isSavingsOk}. ")
        message.append(f"IsWinRateOk: ${isWinRateOk}. ")
        message.append(f"improvementRatio: ${improvementRatio}. ")
        false
      }
    } else {
      modelPromotionGauge.set(0)
      message.append("Couldn't find stats for plutus and plutusDev.")
      false
    }

    val file = new File(metrics_filename)
    val pw = new PrintWriter(file)
    pw.write(ModelPromotionResults(startDateTime, endDateTime, paths, res, message.toString()).asJson.noSpaces)
    pw.close()

    if(result) {
      modelPromotionGauge.set(1)
      copyS3(metrics_filename, f"${sourceBucket}model_logs/${modelVersion}/", recursive = false)
      copyS3(s"${sourceBucket}models/${modelVersion}/", f"${destinationBucket}models/${modelVersion}/")
      copyS3(f"${sourceBucket}models_params/${modelVersion}/", f"${destinationBucket}models_params/${modelVersion}/")
      copyS3(f"${sourceBucket}model_logs/${modelVersion}/", f"${destinationBucket}model_logs/${modelVersion}/")
    } else {
      modelPromotionGauge.set(0)
      copyS3(metrics_filename, f"${trainBucket}model_logs/${modelVersion}/", recursive = false)
      deleteS3(f"${sourceBucket}models_params/${modelVersion}/")
      deleteS3(f"${sourceBucket}model_logs/${modelVersion}/")
      deleteS3(f"${sourceBucket}models/${modelVersion}/")
    }

    if(!isTestEnv())
      prometheus.pushMetrics()
    spark.close()
  }
}
