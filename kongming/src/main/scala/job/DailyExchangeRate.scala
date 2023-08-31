package job


import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{CurrencyExchangeRateDataSet, DailyExchangeRateDataset, DailyExchangeRateRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank, lit}

object DailyExchangeRate extends KongmingBaseJob {

  override def jobName: String = "DailyExchangeRate"
  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val dailyExchangeRate = CurrencyExchangeRateDataSet().readLatestPartitionUpTo(date, isInclusive = true)
      .withColumn("RecencyRank", dense_rank().over(Window.partitionBy($"CurrencyCodeId").orderBy($"AsOfDateUTC".desc)))
      .filter($"RecencyRank"===lit(1))
      .select(
        $"CurrencyCodeId",
        $"FromUSD",
        $"AsOfDateUTC"
      ).orderBy($"CurrencyCodeId".asc)
      .selectAs[DailyExchangeRateRecord]


    val dailyExchangeRateRows = DailyExchangeRateDataset().writePartition(dailyExchangeRate, date, Some(1))

    Array(dailyExchangeRateRows)

  }
}
