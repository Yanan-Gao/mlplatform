package com.thetradedesk.featurestore.configs

import com.thetradedesk.featurestore.testutils.CollectionUtils.compareArrayWithTolerance
import com.thetradedesk.featurestore.testutils.TTDSparkTest

class FeatureDefinitionTest extends TTDSparkTest {
  val precision = 0.000001d

  test("feature default value correct - bool") {
    val feature1 = new FeatureDefinition("n1", DataType.Bool, 0)
    assertResult(false)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Bool, 0, defaultValue = "false")
    assertResult(false)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Bool, 0, defaultValue = "False")
    assertResult(false)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Bool, 0, defaultValue = "true")
    assertResult(true)(feature4.typedDefaultValue)

    val feature5 = new FeatureDefinition("n1", DataType.Bool, 0, defaultValue = "True")
    assertResult(true)(feature5.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Bool, 0, defaultValue = "other")
    assertResult(false)(feature6.typedDefaultValue)
  }

  test("feature default value correct - byte") {
    val feature1 = new FeatureDefinition("n1", DataType.Byte, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = Byte.MaxValue.toString)
    assertResult(Byte.MaxValue)(feature4.typedDefaultValue)

    val feature5 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = Byte.MinValue.toString)
    assertResult(Byte.MinValue)(feature5.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Byte, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - short") {
    val feature1 = new FeatureDefinition("n1", DataType.Short, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = Short.MaxValue.toString)
    assertResult(Short.MaxValue)(feature4.typedDefaultValue)

    val feature5 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = Short.MinValue.toString)
    assertResult(Short.MinValue)(feature5.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Short, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - int") {
    val feature1 = new FeatureDefinition("n1", DataType.Int, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = Int.MaxValue.toString)
    assertResult(Int.MaxValue)(feature4.typedDefaultValue)

    val feature5 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = Int.MinValue.toString)
    assertResult(Int.MinValue)(feature5.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Int, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - long") {
    val feature1 = new FeatureDefinition("n1", DataType.Long, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = Long.MaxValue.toString)
    assertResult(Long.MaxValue)(feature4.typedDefaultValue)

    val feature5 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = Long.MinValue.toString)
    assertResult(Long.MinValue)(feature5.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Long, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - float") {
    val feature1 = new FeatureDefinition("n1", DataType.Float, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = Float.MaxValue.toString)
    assert(math.abs(Float.MaxValue - feature4.typedDefaultValue.asInstanceOf[Double]) < precision)

    val feature5 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = Float.MinValue.toString)
    assert(math.abs(Float.MinValue - feature5.typedDefaultValue.asInstanceOf[Double]) < precision)

    val feature6 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Float, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - double") {
    val feature1 = new FeatureDefinition("n1", DataType.Double, 0)
    assertResult(0)(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = "0")
    assertResult(0)(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = "1")
    assertResult(1)(feature3.typedDefaultValue)

    val feature4 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = Double.MaxValue.toString)
    assert(math.abs(Double.MaxValue - feature4.typedDefaultValue.asInstanceOf[Double]) < precision)

    val feature5 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = Double.MinValue.toString)
    assert(math.abs(Double.MinValue - feature5.typedDefaultValue.asInstanceOf[Double]) < precision)

    val feature6 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = "-1")
    assertResult(-1)(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Double, 0, defaultValue = "other")
    assertResult(0)(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed byte array") {
    val feature1 = new FeatureDefinition("n1", DataType.Byte, 3)
    assertResult(new Array[Long](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Byte, 2, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Byte, 2, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Byte, 3, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Byte, 3, defaultValue = "other")
    assertResult(new Array[Long](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed short array") {
    val feature1 = new FeatureDefinition("n1", DataType.Short, 3)
    assertResult(new Array[Long](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Short, 2, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Short, 2, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Short, 3, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Short, 3, defaultValue = "other")
    assertResult(new Array[Long](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed int array") {
    val feature1 = new FeatureDefinition("n1", DataType.Int, 3)
    assertResult(new Array[Long](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Int, 2, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Int, 2, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Int, 3, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Int, 3, defaultValue = "other")
    assertResult(new Array[Long](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed long array") {
    val feature1 = new FeatureDefinition("n1", DataType.Long, 3)
    assertResult(new Array[Long](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Long, 2, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Long, 2, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Long, 3, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.byteValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Long, 3, defaultValue = "other")
    assertResult(new Array[Long](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed float array") {
    val feature1 = new FeatureDefinition("n1", DataType.Float, 3)
    assertResult(new Array[Double](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Float, 2, defaultValue = "[0, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), 1.001.doubleValue()), feature2.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature3 = new FeatureDefinition("n1", DataType.Float, 2, defaultValue = "[10.001, -10.001]")
    compareArrayWithTolerance[Double](Array[Double](10.001.doubleValue(), -10.001.doubleValue()), feature3.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature6 = new FeatureDefinition("n1", DataType.Float, 3, defaultValue = "[0, -1.001, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), -1.001.doubleValue(), 1.001.doubleValue()), feature6.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature7 = new FeatureDefinition("n1", DataType.Float, 3, defaultValue = "other")
    assertResult(new Array[Double](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - fixed double array") {
    val feature1 = new FeatureDefinition("n1", DataType.Double, 3)
    assertResult(new Array[Double](3))(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Double, 2, defaultValue = "[0, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), 1.001.doubleValue()), feature2.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature3 = new FeatureDefinition("n1", DataType.Double, 2, defaultValue = "[10.001, -10.001]")
    compareArrayWithTolerance[Double](Array[Double](10.001.doubleValue(), -10.001.doubleValue()), feature3.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature6 = new FeatureDefinition("n1", DataType.Double, 3, defaultValue = "[0, -1.001, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), -1.001.doubleValue(), 1.001.doubleValue()), feature6.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature7 = new FeatureDefinition("n1", DataType.Double, 3, defaultValue = "other")
    assertResult(new Array[Double](3))(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic byte array") {
    val feature1 = new FeatureDefinition("n1", DataType.Byte, 1)
    assertResult(Array.empty[Long])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Byte, 1, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Byte, 1, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Byte, 1, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Byte, 1, defaultValue = "other")
    assertResult(Array.empty[Long])(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic short array") {
    val feature1 = new FeatureDefinition("n1", DataType.Short, 1)
    assertResult(Array.empty[Long])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Short, 1, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Short, 1, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Short, 1, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Short, 1, defaultValue = "other")
    assertResult(Array.empty[Long])(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic int array") {
    val feature1 = new FeatureDefinition("n1", DataType.Int, 1)
    assertResult(Array.empty[Long])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Int, 1, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Int, 1, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Int, 1, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.longValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Int, 1, defaultValue = "other")
    assertResult(Array.empty[Long])(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic long array") {
    val feature1 = new FeatureDefinition("n1", DataType.Long, 1)
    assertResult(Array.empty[Long])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Long, 1, defaultValue = "[0, 1]")
    assertResult(Array[Long](0.longValue(), 1.longValue()))(feature2.typedDefaultValue)

    val feature3 = new FeatureDefinition("n1", DataType.Long, 1, defaultValue = "[10, -10]")
    assertResult(Array[Long](10.longValue(), -10.longValue()))(feature3.typedDefaultValue)

    val feature6 = new FeatureDefinition("n1", DataType.Long, 1, defaultValue = "[0, -1, 1]")
    assertResult(Array[Long](0.longValue(), -1.longValue(), 1.byteValue()))(feature6.typedDefaultValue)

    val feature7 = new FeatureDefinition("n1", DataType.Long, 1, defaultValue = "other")
    assertResult(Array.empty[Long])(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic float array") {
    val feature1 = new FeatureDefinition("n1", DataType.Float, 1)
    assertResult(Array.empty[Double])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Float, 1, defaultValue = "[0, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), 1.001.doubleValue()), feature2.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature3 = new FeatureDefinition("n1", DataType.Float, 1, defaultValue = "[10.001, -10.001]")
    compareArrayWithTolerance[Double](Array[Double](10.001.doubleValue(), -10.001.doubleValue()), feature3.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature6 = new FeatureDefinition("n1", DataType.Float, 1, defaultValue = "[0, -1.001, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), -1.001.doubleValue(), 1.001.doubleValue()), feature6.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature7 = new FeatureDefinition("n1", DataType.Float, 1, defaultValue = "other")
    assertResult(Array.empty[Double])(feature7.typedDefaultValue)
  }

  test("feature default value correct - dynamic double array") {
    val feature1 = new FeatureDefinition("n1", DataType.Double, 1)
    assertResult(Array.empty[Double])(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.Double, 1, defaultValue = "[0, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), 1.001.doubleValue()), feature2.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature3 = new FeatureDefinition("n1", DataType.Double, 1, defaultValue = "[10.001, -10.001]")
    compareArrayWithTolerance[Double](Array[Double](10.001.doubleValue(), -10.001.doubleValue()), feature3.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature6 = new FeatureDefinition("n1", DataType.Double, 1, defaultValue = "[0, -1.001, 1.001]")
    compareArrayWithTolerance[Double](Array[Double](0.doubleValue(), -1.001.doubleValue(), 1.001.doubleValue()), feature6.typedDefaultValue.asInstanceOf[Array[Double]], precision)

    val feature7 = new FeatureDefinition("n1", DataType.Double, 1, defaultValue = "other")
    assertResult(Array.empty[Double])(feature7.typedDefaultValue)
  }

  test("feature default value correct - string") {
    val feature1 = new FeatureDefinition("n1", DataType.String)
    assertResult("")(feature1.typedDefaultValue)

    val feature2 = new FeatureDefinition("n1", DataType.String, 0, defaultValue = "random data")
    assertResult("random data")(feature2.typedDefaultValue)
  }
}
