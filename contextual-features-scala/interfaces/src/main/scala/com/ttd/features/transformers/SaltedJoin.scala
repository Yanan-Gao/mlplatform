package com.ttd.features.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * Same as Join, but with salt.
 */
class SaltedJoin(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = {
    this(Identifiable.randomUID("Join"))
  }

  final val rightTempName = new Param[String](this, "rightTempName", "The smaller right table temp name")
  final val joinType = new Param[String](this, "joinType", "The join type")
  final val joinCols = new StringArrayParam(this, "joinCols", "The join columns")
  final val salt = new IntParam(this, "salt", "Salt amount")

  private def _transform(left: Dataset[_], right: Dataset[_]): DataFrame = {
    SaltedJoin.saltedJoin(left, right, $(joinCols), $(joinType), $(salt))
  }

  override def transform(ds: Dataset[_]): DataFrame = {
    val right = ds.sparkSession.table("global_temp." + $(rightTempName))
    _transform(ds, right)
  }

  override def transformSchema(schema: StructType): StructType = {
    val l = SparkSession.active.createDataFrame(List[Row]().asJava, schema)
    val r = SparkSession.active.createDataFrame(
      List[Row]().asJava,
      SparkSession.active.table("global_temp." + $(rightTempName)).schema
    )
    _transform(l, r).schema
  }

  override def copy(extra: ParamMap): this.type = {
    defaultCopy(extra)
  }
}

object SaltedJoin extends DefaultParamsReadable[SaltedJoin] {
  def apply(rightTempName: String, cols: Array[String], joinType: String = "inner", salt: Int = 100): SaltedJoin = {
    val join = new SaltedJoin()
    join.set(join.rightTempName -> rightTempName)
    join.set(join.joinCols -> cols)
    join.set(join.joinType -> joinType)
    join.set(join.salt -> salt)
  }

  def apply(rightTempName: String, col: String, joinType: String, salt: Int): SaltedJoin = {
    val join = new SaltedJoin()
    join.set(join.rightTempName -> rightTempName)
    join.set(join.joinCols -> Array(col))
    join.set(join.joinType -> joinType)
    join.set(join.salt -> salt)
  }

  // df should be the bigger dataset
  def saltedJoin(buildDf: Dataset[_], df: Dataset[_], joinCols: Seq[String], joinType: String, salt: Int): DataFrame = {
    import org.apache.spark.sql.functions._
    val tmpDf = buildDf.withColumn("slt_range", array(Range(0, salt).toList.map(lit): _*))

    val tableDf = tmpDf.withColumn("slt_ratio", explode(tmpDf("slt_range"))).drop("slt_range")
    val streamDf = df.withColumn("slt_ratio", monotonically_increasing_id % salt)

    val saltedExpr = joinCols ++ Seq("slt_ratio")
    streamDf.join(tableDf, saltedExpr, joinType).drop("slt_ratio")
  }
}
