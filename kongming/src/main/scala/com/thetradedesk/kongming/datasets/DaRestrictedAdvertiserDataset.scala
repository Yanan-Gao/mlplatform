package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.date
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import org.apache.spark.sql.Dataset

final case class DaRestrictedAdvertiserRecord(AdvertiserId: String,
                                              CategoryPolicy: String,
                                              IsRestricted: Int
                                             )

case class DaRestrictedAdvertiserDataset() extends ProvisioningS3DataSet[DaRestrictedAdvertiserRecord]("distributedalgosadvertiserrestrictionstatus/v=1"){}


final case class ClickPartnerExcludeRecord(PartnerId: String)

final case class AdvertiserExcludeRecord(AdvertiserId: String)

object ClickPartnerExcludeList {
  val S3Path: String = "s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/philo/partnerFilter/partner_exclusion_list.csv"

  val partnerListDf: Dataset[ClickPartnerExcludeRecord] = spark.read
    .options(Map("header" -> "false"))
    .csv(s"${S3Path}")
    .toDF("PartnerId")
    .selectAs[ClickPartnerExcludeRecord]

  def readAdvertiserList(): Dataset[AdvertiserExcludeRecord] = {
    val advertiserDf = AdvertiserDataSet().readLatestPartitionUpTo(date).select("AdvertiserId", "PartnerId")

    partnerListDf.join(advertiserDf, Seq("PartnerId"), "inner").selectAs[AdvertiserExcludeRecord]
  }
}