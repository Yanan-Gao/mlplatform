package com.thetradedesk

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashMap

package object philo {

  /** shift hashed value to positive and reserve 0 for null values
   * copied from plutus
   *
   * @param hashValue value generated from xxhash64
   * @cardinality cardinality for the hash function
   * @return shifed value for hashing results
   */
  def shiftMod(hashValue: Long, cardinality: Int): Int = {
    val modulo = math.min(cardinality - 1, Int.MaxValue - 1)
    val index = (hashValue % modulo).intValue()
    // zero index is reserved for UNK and we do not want negative values
    val shift = if (index < 0) modulo + 1 else 1
    index + shift
  }
  /** convert it to a spark udf function
   * @return udf function for shiftMod
   */
  def shiftModUdf: UserDefinedFunction = udf((hashValue: Long, cardinality: Int) => {
    shiftMod(hashValue, cardinality)
  })



  def flattenData(data: DataFrame, flatten_set: Seq[String]): DataFrame = {
    data.select(
      data.columns.map(
        c => if (flatten_set.contains(c))
          col(s"${c}.value").alias(c).alias(c)
        else col(c)
      ): _*
    )
  }

}
