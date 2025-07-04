package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Merger {

  def joinDataSets(df1: Dataset[_], df2: Dataset[_], joinType: String = "inner"): Dataset[_] = {
    if (isProfileHighDim) {
      df1.join(df2, Seq("FeatureKey"), joinType)
    } else {
      df1.join(broadcast(df2), Seq("FeatureKey"), joinType)
    }
  }

  def joinDataFrames(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
    if (isProfileHighDim) {
      df1.join(df2, Seq("FeatureKey"), joinType)
    } else {
      df1.join(broadcast(df2), Seq("FeatureKey"), joinType)
    }
  }

  def unionDataFrames(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.union(df2)
  }

}
