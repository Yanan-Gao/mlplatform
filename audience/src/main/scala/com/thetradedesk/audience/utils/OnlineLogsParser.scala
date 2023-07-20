package com.thetradedesk.audience.utils

import io.circe.parser.parse
import io.circe.{Decoder, Json}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

// case classes used for parsing online logs
case class FeatureDimensions(Dimensions: List[Int])
case class ModelFeature(Cardinality: Int, Name: String, Shape: FeatureDimensions)
case class FeatureDefinitionElement(FeatureDefinitions: List[ModelFeature], ModelVersion: Int)
case class OnlineLogFeatureJson(Array: List[Float], Shape: FeatureDimensions)
case class FeaturesSchema(ModelFeatureDefinitions: List[FeatureDefinitionElement])

// wrap all the parsing functions in a serializable object
object OnlineLogsParser extends Serializable {
  implicit val featureDimensionsDecoder: Decoder[FeatureDimensions] = Decoder.forProduct1("Dimensions")(FeatureDimensions.apply)
  implicit val modelFeatureDecoder: Decoder[ModelFeature] = Decoder.forProduct3("Cardinality", "Name", "Shape")(ModelFeature.apply)
  implicit val featureDefinitionElementDecoder: Decoder[FeatureDefinitionElement] = Decoder.forProduct2("FeatureDefinitions", "ModelVersion")(FeatureDefinitionElement.apply)
  implicit val featuresSchemaDecoder: Decoder[FeaturesSchema] = Decoder.forProduct1("ModelFeatureDefinitions")(FeaturesSchema.apply)
  implicit val onlineLogFeatureJsonDecoder: Decoder[OnlineLogFeatureJson] = Decoder.forProduct2("Array", "Shape")(OnlineLogFeatureJson.apply)

  def extractFeatureNames(featuresSchema: String): List[String] = {
    parse(featuresSchema)
      .getOrElse(null)
      .as[Map[String, List[FeatureDefinitionElement]]]
      .getOrElse(null)
      .getOrElse("ModelFeatureDefinitions", null).head
      .FeatureDefinitions
      .map(_.Name)
  }

  def parseFeatureJsonUDF: UserDefinedFunction = udf((featureJsonStr: String) => {
    val json: Json = parse(featureJsonStr).getOrElse(null)
    json.as[Map[String, OnlineLogFeatureJson]].getOrElse(null)
  })

  // TODO: add serializable null handling in the following UDFs
  def extractFeatureUDF: UserDefinedFunction = udf((featuresParsed: Map[String, OnlineLogFeatureJson], feature: String) => {
    val featureValue = featuresParsed.getOrElse(feature, null)
    // if (featureValue == null) None else Some(featureValue.Array.head)
    featureValue.Array.head
  })

  def extractDynamicFeatureUDF: UserDefinedFunction = udf((featureJsonStr: String, feature: String) => {
    val parsedJson: Map[String, String] = parse(featureJsonStr).getOrElse(null).as[Map[String, String]].getOrElse(null)
    val jsonValue = parsedJson.getOrElse(feature, null)
    // if (jsonValue == null) None else {
    //   if (jsonValue.contains(",")) Some(jsonValue.split(",").map(_.toLong)) else Some(Array(jsonValue.toLong))
    // }
    if (jsonValue.contains(",")) jsonValue.split(",").map(_.toLong) else Array(jsonValue.toLong)
  })
}
