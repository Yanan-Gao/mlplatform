package com.thetradedesk.plutus.data
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.time.LocalDate

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
    val expected = "year=2021/month=1/day=1"
    val result = explicitDatePart(LocalDate.of(2021, 1, 1))
    assert(expected == result)
  }

  "parquetDataPaths" should "create a path with date=yyyymmdd" in {
    val expected = Seq("s3_path/date=20210101")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1))
    assertResult(expected)(result)
  }

  "parquetDataPaths" should "create a list of paths with a lookback ignoring order" in {
    val expected = Seq("s3_path/date=20210101", "s3_path/date=20210102", "s3_path/date=20210103")
    val result = parquetDataPaths("s3_path", LocalDate.of(2021, 1, 3), lookBack = Some(2))
    result == expected
  }

  "cleansedDataPaths" should "create paths with s3/yyyy/mm/dd/*/*/*.gz" in {
    val expected = Seq("s3_path/date=20210101")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1))
    assertResult(expected)(result)
  }

  "parquetDataPaths for Plutus" should "create paths with s3/year=yyyy/month=mm/day=dd" in {
    val expected = Seq("s3_path/year=2021/month=1/day=1")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1), source = Some(PLUTUS_DATA_SOURCE))
    assertResult(expected)(result)
  }
  "parquetDataPaths for Plutus" should "create multiple paths with s3/year=yyyy/month=mm/day=dd" in {
    val expected = Seq("s3_path/year=2021/month=1/day=1", "s3_path/year=2020/month=12/day=31", "s3_path/year=2020/month=12/day=30")
    val result = parquetDataPaths("s3_path", date=LocalDate.of(2021, 1, 1), source = Some(PLUTUS_DATA_SOURCE), lookBack = Some(2))
    result == expected
  }

  "plutusDataPath" should "return the path to s3 for clean data" in {
    val expected = "s3://bucket/env/prefix/google/year=2021/month=1/day=1/"
    val result = plutusDataPath(s3Path = "s3://bucket", ttdEnv = "env", prefix = "raw", svName = Some("google"), date = LocalDate.of(2021, 1, 1))
    expected == result
  }

  "plutusDataPaths" should "return list of path to s3 for clean data" in {
    val expected = Seq("s3://bucket/env/prefix/google/year=2021/month=1/day=1/", "s3://bucket/env/prefix/google/year=2020/month=12/day=31/", "s3://bucket/env/prefix/google/year=2020/month=12/day=30/")
    val result = plutusDataPaths(s3Path = "s3://bucket", ttdEnv = "env", prefix = "raw", svName = Some("google"), date = LocalDate.of(2021, 1, 1), lookBack = Some(2))
    expected == result
  }

  "shiftMod" should "move the value up to allow unk at zero" in {
    val expected = 5959
    val sparkMagicSeedNumber = 42L
    val s = org.apache.spark.unsafe.types.UTF8String.fromString("ttd.com")
    val hash = org.apache.spark.sql.catalyst.expressions.XXH64.hashUnsafeBytes(s.getBaseObject, s.getBaseOffset, s.numBytes(), sparkMagicSeedNumber)
    val result = shiftMod(hash, 10000)
    println(result)
    assertResult(expected)(result)
  }

  "shiftMod zero" should "move the value up to allow unk at zero" in {
    val expected = 1
    val index = 0
    val result = shiftMod(index, 5)
    println(result)
    assertResult(expected)(result)
  }
}

