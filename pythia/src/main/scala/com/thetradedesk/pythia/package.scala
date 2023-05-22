package com.thetradedesk

import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, explicitDatePart, paddedDatePart}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.time.LocalDate

package object pythia {
  /*
  * copied from philo / plutus
  * applicable to both, Pythia interest and demo model
  */

  // NOTE: this is TF record and you will need to add tf record package to the packages args when running spark submit for this to work
  def writeData(df: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate,
                partitions: Int, isTFRecord: Boolean = true): Unit = {

    // note the date part is year=yyyy/month=m/day=d/
    var func = df
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)

    if (isTFRecord) {
      func.format("tfrecords")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}")
    } else {
      func.option("header", "true")       
        .csv(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}")
    }
  }
}