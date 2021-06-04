import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

@Ignore
class ExampleSpec extends AnyFlatSpec with Matchers {
/*
  "Test run" should "not throw an error" in {

    val conf = new SparkConf().setAppName("TestFSClient").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()


    val clientConfig = new FeatureStoreClientConfig
    val client = new FeatureStoreClient(clientConfig)

    val ds = client.ReadFeature(spark, 1L, 1L, 1L)

    ds.printSchema()


  }
*/

}