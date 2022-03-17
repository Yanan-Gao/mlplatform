package com.ttd.mycellium.drivers

import com.ttd.features.FeatureConfig
import com.ttd.features.datasets.{BidRequest, ReadableDataFrame}
import com.ttd.features.util.PipelineUtils._
import com.ttd.mycellium.pipelines.bidrequest._
import com.ttd.mycellium.spark.TTDSparkContext.spark
import com.ttd.mycellium.spark.config.TTDConfig.config
import com.ttd.mycellium.util.logging.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}

class BidRequestConfig extends FeatureConfig {
  val outputDate: DateTime = DateTime.parse(config.getString("outputDate", DateTime.now.toString))

  val dateFormat = "yyyyMMdd"
  val bidRequest: ReadableDataFrame = BidRequest()
  override def sources: Map[String, Seq[DateTime]] = Map(
    bidRequest.basePath -> Seq(outputDate)
  )
  val saveMode: SaveMode = SaveMode.Overwrite

  val outputBasePath = "s3://thetradedesk-useast-hadoop/Data_Science/christopher.hawkes/application/feature/mycellium"
}

class BidRequestDriver(val config: BidRequestConfig) extends Runnable with Logger {
  override def run(): Unit = {
    val outputDate = s"/date=${config.outputDate.toString(config.dateFormat)}"
    val br = config.bidRequest.read(Seq(config.outputDate))(spark)

    val brInputPipe = new BidRequestInputPipe()
    val dataFrame = brInputPipe.fitTransform(br)
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val jobs: ParSeq[(Pipeline, String)] = Seq(
      // edges
      (new TrackedAtEdgesPipe(),               "/edge/type=TrackedAt"),
      (new ConnectedFromEdgesPipe(),           "/edge/type=ConnectedFrom"),
      (new BrowsesEdgesPipe(),                 "/edge/type=Browses"),
      (new GeoContainsEdgesPipe(),             "/edge/type=GeoContains"),
      (new BidsEdgesPipe(),                    "/edge/type=Bids"),
      (new AdvertiserOwnsEdgesPipe(),          "/edge/type=AdvertiserOwns"),
      (new AdvertiserRunsCampaignEdgesPipe(),  "/edge/type=AdvertiserRuns"),
      (new PublisherHasSiteEdgesPipe(),        "/edge/type=PublisherHasSite"),
      (new SupplyVendorHasPublisherEdgesPipe(),"/edge/type=SupplyVendorHasPublisher"),
      // vertexes
      (new LatLongVertexPipe(),                "/vertex/type=LatLong"),
      (new IPAddressVertexPipe(),              "/vertex/type=IPAddress"),
      (new SiteVertexPipe(),                   "/vertex/type=Site"),
      (new GeoLocationVertexPipe(),            "/vertex/type=GeoLocation"),
      (new UserIDVertexPipe(),                     "/vertex/type=ID"),
      (new AdGroupVertexPipe(),                "/vertex/type=AdGroup"),
      (new AdvertiserVertexPipe(),             "/vertex/type=Advertiser"),
      (new PublisherVertexPipe(),              "/vertex/type=Publisher"),
      (new SupplyVendorVertexPipe(),           "/vertex/type=SupplyVendor"),
      (new CreativeVertexPipe(),               "/vertex/type=Creative"),
      (new CampaignVertexPipe(),                "/vertex/type=Campaign"),
    ).par
    val forkJoinPool = new java.util.concurrent.ForkJoinPool(2)
    jobs.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    jobs.foreach{case (pipe, path) => {
      pipe.fitTransform(dataFrame)
        .write.mode(config.saveMode).parquet(config.outputBasePath + path + outputDate)
      log.info(path + " Written")
    }}

    forkJoinPool.shutdown()

//    // write edges
//    new TrackedAtEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=TrackedAt" + outputDate)
//    log.info("TrackedAt Written")
//
//    new ConnectedFromEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=ConnectedFrom" + outputDate)
//    log.info("ConnectedFrom Written")

//    new BrowsesEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=Browses" + outputDate)
//    log.info("Browses Written")

//    new GeoContainsEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=GeoContains" + outputDate)
//    log.info("GeoContains Written")

//    new BidsEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=Bids" + outputDate)
//    log.info("Bids Written")

//    new AdvertiserOwnsEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=AdvertiserOwns" + outputDate)
//    log.info("AdvertiserOwns Written")

//    new AdvertiserRunsCampaignEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=AdvertiserRuns" + outputDate)
//    log.info("AdvertiserRuns Written")

//    new PublisherHasSiteEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=PublisherHasSite" + outputDate)
//    log.info("PublisherHasSite Written")

//    new SupplyVendorHasPublisherEdgesPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/edge/type=SupplyVendorHasPublisher" + outputDate)
//    log.info("SupplyVendorHasPublisher Written")
//
//    // write vertexes
//    new LatLongVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=LatLong" + outputDate)
//    log.info("LatLong Written")
//
//    new IPAddressVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=IPAddress" + outputDate)
//    log.info("IPAddress Written")
//
//    new SiteVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=Site" + outputDate)
//    log.info("Site Written")
//
//    new GeoLocationVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=GeoLocation" + outputDate)
//    log.info("GeoLocation Written")
//
//    new IDVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=ID" + outputDate)
//    log.info("ID Written")

//    new AdGroupVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=AdGroup" + outputDate)
//    log.info("AdGroup Written")
//
//    new AdvertiserVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=Advertiser" + outputDate)
//    log.info("Advertiser Written")

//    new PublisherVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=Publisher" + outputDate)
//    log.info("Publisher Written")
//
//    new SupplyVendorVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=SupplyVendor" + outputDate)
//    log.info("SupplyVendor Written")

//    new CreativeVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=Creative" + outputDate)
//    log.info("Creative Written")

//    new CampaignVertexPipe().fitTransform(dataFrame)
//      .write.mode(config.saveMode).parquet(config.outputBasePath + "/vertex/type=Campaign" + outputDate)
//    log.info("Campaign Written")

    dataFrame.unpersist()
  }
}

object BidRequestDriver {
  def main(args: Array[String]): Unit = {
    new BidRequestDriver(new BidRequestConfig).run()
  }
}

