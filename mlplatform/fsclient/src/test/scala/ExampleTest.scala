import com.thetradedesk.mlplatform.fsclient.{FeatureStoreClient, FeatureStoreClientConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.Ignore

import collection.mutable.Stack
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

@Ignore
class ExampleSpec extends AnyFlatSpec with Matchers {

  "Test run" should "not throw an error" in {

    val conf = new SparkConf().setAppName("TestFSClient").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()


    val clientConfig = new FeatureStoreClientConfig
    val client = new FeatureStoreClient(clientConfig)

    val ds = client.ReadFeature(spark, 1L, 1L, 1L)

    ds.printSchema()


  }


}