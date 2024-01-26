package com.thetradedesk.plutus.data
import com.thetradedesk.plutus.data.schema.{PlutusLogsDataset, RawLostBidDataset}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.time.{LocalDate, LocalDateTime}

class packageTest extends AnyFlatSpec {


  "paddedDatePart with slashes" should "create a date in yyyy/mm/dd format with leading zero" in {
    val expected = "2021/01/01"
    val result = paddedDatePart(LocalDate.of(2021, 1, 1), separator = Some("/"))
    assert(expected == result)
  }

  "paddedDatePartNoSlashes" should "create a date in yyyy/mm/dd format with leading zero" in {
    val expected = "20210101"
    val result = paddedDatePart(LocalDate.of(2021, 1, 1))
    assert(expected == result)
  }

  "explicitDatePart" should "create a date in year=yyyy month=mm day-dd format with leading zero" in {
    val expected = "year=2021/month=01/day=01"
    val result = explicitDatePart(LocalDate.of(2021, 1, 1))
    assert(expected == result)
  }

  "parquetDataPaths" should "create a path with date=yyyymmdd" in {
    val expected = Seq("s3_path/date=20210101")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1))
    assertResult(expected)(result)
  }

  "parquetDataPaths" should "create a list of paths with a lookback ignoring order" in {
    val expected = Vector("s3_path/date=20210101", "s3_path/date=20210102", "s3_path/date=20210103")
    val result = parquetDataPaths("s3_path", LocalDate.of(2021, 1, 3), lookBack = Some(2))
    expected should contain theSameElementsAs (result)
  }

  "cleansedDataPaths" should "create paths with s3/yyyy/mm/dd/*/*/*.gz" in {
    val expected = Seq("s3_path/date=20210101")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1))
    assertResult(expected)(result)
  }

  "parquetDataPaths for Plutus" should "create paths with s3/year=yyyy/month=mm/day=dd" in {
    val expected = Seq("s3_path/year=2021/month=01/day=01")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1), source = Some(PLUTUS_DATA_SOURCE))
    assertResult(expected)(result)
  }

  "parquetDataPaths for Plutus" should "create multiple paths with s3/year=yyyy/month=mm/day=dd" in {
    val expected = Seq("s3_path/year=2021/month=01/day=01", "s3_path/year=2020/month=12/day=31", "s3_path/year=2020/month=12/day=30")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1), source = Some(PLUTUS_DATA_SOURCE), lookBack = Some(2))
    expected should contain theSameElementsAs result
  }

  "plutusDataPath" should "return the path to s3 for clean data" in {
    val expected = "s3://bucket/env/raw/google/year=2021/month=01/day=01"
    val result = plutusDataPath(s3Path = "s3://bucket", ttdEnv = "env", prefix = "raw", svName = Some("google"), date = LocalDate.of(2021, 1, 1))
    assertResult(expected)(result)
  }

  "plutusDataPaths" should "return list of path to s3 for clean data" in {
    val env = "env"
    val prefix = "raw"
    val result = plutusDataPaths(s3Path = "s3://bucket", ttdEnv = "env", prefix = "raw", svName = Some("google"), date = LocalDate.of(2021, 1, 1), lookBack = Some(1))
    result should contain theSameElementsAs Seq(f"s3://bucket/$env/$prefix/google/year=2021/month=01/day=01", f"s3://bucket/$env/$prefix/google/year=2020/month=12/day=31")
  }

  "plutusDataPaths" should "return single path with no lookback" in {
    val env = "env"
    val prefix = "raw"
    val result = plutusDataPaths(s3Path = "s3://bucket", ttdEnv = "env", prefix = "raw", svName = Some("google"), date = LocalDate.of(2021, 1, 1))
    result should contain theSameElementsAs Seq(f"s3://bucket/$env/$prefix/google/year=2021/month=01/day=01")
  }

  "modulo" should "return positive values" in {
//    https://stackoverflow.com/questions/70353631/rabin-karp-algorithm-negative-hash
    val expected = 5
    val modulo = Int.MaxValue
    val value = Long.MinValue

    println(modulo)
    println(value)
    println(value%modulo)
    println((value % modulo + modulo) % modulo)
    val v = value % modulo
    if(v < 0) println(v + modulo) else println(v)

    println((-1 % modulo + modulo) % modulo)

    println(nonNegativeMod((value % modulo).intValue(), modulo))
    println(nonNegativeModulo(value, Some(modulo)))

    // if we want to keep 0 for UNK we need to have modulo be Int.MaxValue - 1
    // Integer is 2^31 (signed) = 2,147,483,647
    // UNK allowed gives 2^30 = 1,073,741,824

  }

  "parquetHourlyDataPaths" should "create a single paths with s3/date=yyyymmdd/hour=h" in {
    val expected = Seq("s3_path/date=20210101/hour=2")
    val result = parquetHourlyDataPaths(
      "s3_path",
      dateTime=LocalDateTime.of(2021, 1, 1, 2, 30),
      source = None,
      lookBack = None
    )
    expected should contain theSameElementsAs result
  }


  "parquetHourlyDataPaths" should "create three paths with s3/date=yyyymmdd/hour=h" in {
    val expected = Seq("s3_path/date=20210101/hour=1", "s3_path/date=20210101/hour=0", "s3_path/date=20201231/hour=23", "s3_path/date=20201231/hour=22")
    val result = generateDataPathsHourly(
      "s3_path/",
      PlutusLogsDataset.S3PATH_GEN,
      dateTime=LocalDateTime.of(2021, 1, 1, 1, 30),
      lookBack = Some(3)
    )
    expected should contain theSameElementsAs result
  }


  "parquetHourlyDataPaths" should "create multiple paths with s3/year=yyyy/month=mm/day=dd/hour=h" in {
    val expected = Seq("s3_path/year=2021/month=01/day=01/hourPart=1", "s3_path/year=2021/month=01/day=01/hourPart=0", "s3_path/year=2020/month=12/day=31/hourPart=23")
    val result = parquetHourlyDataPaths(
      "s3_path",
      dateTime=LocalDateTime.of(2021, 1, 1, 1, 30),
      source = Some(IMPLICIT_DATA_SOURCE),
      lookBack = Some(2)
    )
    expected should contain theSameElementsAs result
  }

  "cleansedDataPathsHourly" should "create multiple paths with s3/year=yyyy/month=mm/day=dd/hour=h" in {

    val expected = Seq("s3://thetradedesk-useast-logs-2/lostbidrequest/cleansed/2024/01/01/23/*/*.gz")
    val result = generateDataPathsHourly(
      RawLostBidDataset.S3PATH,
      RawLostBidDataset.S3PATH_GEN,
      dateTime=LocalDateTime.of(2024, 1, 1, 23, 0)
    )
    expected should contain theSameElementsAs result
  }



  "isOkay" should "return false when stageStat is less than prodStat" in {
    isOkay(10.0, 8.0) should be(false)
  }

  it should "return true when stageStat is just above threshold" in {
    isOkay(10.0, 9.51) should be(true)
  }

  it should "return false when stageStat is right below threshold" in {
    isOkay(10.0, 9.5) should be(false)
  }

  it should "return true when stageStat is above threshold" in {
    isOkay(10.0, 10.9) should be(true)
  }

  it should "return false when using custom margin below threshold" in {
    isOkay(100.0, 90.0, 0.1) should be(false)
  }

  it should "return true when using custom margin at threshold" in {
    isOkay(100.0, 90.1, 0.1) should be(true)
  }

  "isBetter" should "return true over threshold" in {
    isBetter(100.0, 100.1) should be(true)
  }

  it should "return true at threshold" in {
    isBetter(100.0, 100.0) should be(true)
  }

  it should "return false under threshold" in {
    isBetter(100.0, 99.9) should be(false)
  }
}

