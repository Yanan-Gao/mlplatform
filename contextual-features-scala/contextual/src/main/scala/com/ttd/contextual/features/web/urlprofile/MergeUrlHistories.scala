package com.ttd.contextual.features.web.urlprofile

import com.ttd.contextual.features.keywords.yake.YakeKeywordFinisher
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{collect_list, udf}

case class UrlHistoryElem(count: Long, ts: Long, url: String)

class MergeUrlHistories extends Transformer {
  override val uid: String = Identifiable.randomUID("MergeUrlHistories")

  final val uiidCol = new Param[String](this, "uiidCol", "The uiid column")
  final val historyCol = new Param[String](this, "historyCol", "The history column")

  val aggFlatten: UserDefinedFunction = udf((seq: Seq[Seq[(Long, Long, String)]]) => {
     seq.flatten.groupBy(_._3)
       .mapValues(vs => (vs.map(_._1).sum, vs.map(_._2).max))
       .map{case (k, (c, ts)) => UrlHistoryElem(c,ts,k)}
       .toSeq.sortBy(-_.ts).take(1000)
  })

  def setUiidCol(value: String): this.type = set(uiidCol, value)
  setDefault(uiidCol -> "uiid")
  def setHistoryCol(value: String): this.type = set(historyCol, value)
  setDefault(historyCol -> "history")

  override def transform(ds: Dataset[_]): DataFrame = {
    ds
      .groupBy($(uiidCol))
      .agg(aggFlatten(collect_list($(historyCol)) as $(historyCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): YakeKeywordFinisher = {
    defaultCopy(extra)
  }
}
