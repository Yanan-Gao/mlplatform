package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{loadModelFeatures, loadParquetData}

import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.io.FSUtils.fileExists

import org.apache.spark.sql.DataFrame
import java.time.LocalDate

import com.thetradedesk.pythia.interestModel.schema.{ModelInputRecord, ModelInputDataset}
import com.thetradedesk.pythia.interestModel.schema.{interestLabelTaxonomyRecord, interestLabelTaxonomyDataset}
import com.thetradedesk.pythia.interestModel.transform.ModelInputTransform
import com.thetradedesk.pythia.writeData

object interestModelInput {
  val date = config.getDate("date", LocalDate.now().withDayOfMonth(1)) // default to first day of the current month

  val outputPath = config.getString("outputPath", "s3://thetradedesk-mlplatform-us-east-1/features/data/pythia/interests/v=1/")
  val ttdEnv = config.getString("ttd.env", "dev")

  val featuresJson = config.getString("featuresJson", default="s3://thetradedesk-mlplatform-us-east-1/libs/pythia/interests/features/pythia-interestModel-features.json")

  val outputPartitions = config.getInt("partitions", 100) // if the sampling factor is changed, this default should be adjusted
  
  def main(args: Array[String]): Unit = {
    val readEnv = "prod"
    val writeEnv = if (ttdEnv == "prodTest") "dev" else ttdEnv

    val brBfLoc = BidsImpressions.BIDSIMPRESSIONSS3 + f"${readEnv}/bidsimpressions/"

    val dfBidsImpressions = loadParquetData[BidsImpressionsSchema](brBfLoc, date, source = Some("geronimo"))

    val dfTaxMapFull = spark.read.option("inferSchema","true").option("sep", "\t").option("header", "true").csv(interestLabelTaxonomyDataset.TAXS3) 
    // TO DO: instead of inferring the schema, read with defined schema

    val modelFeatures = loadModelFeatures(featuresJson)
  
    // Creating the labeled data set and the ordered sequence of labels/label names
    val (dfHashedWithLabelSampled, columnsLabels) = ModelInputTransform.transform(dfBidsImpressions, dfTaxMapFull, modelFeatures)

    // Saving the sampled, labeled & hashed data output as TF records
    writeData(dfHashedWithLabelSampled, outputPath, writeEnv, "interestModelInput", date, outputPartitions)

    // Saving (the order of) the labels
    columnsLabels.toSeq.toDF.coalesce(1).write.option("header","false").mode("overwrite").csv(s"${outputPath}/${writeEnv}/interestLabels/${date}_labels")

    // TO DO: Create overview counts over the labels for monitoring here?
  }

}