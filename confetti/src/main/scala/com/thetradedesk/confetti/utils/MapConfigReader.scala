package com.thetradedesk.confetti.utils

import java.time.LocalDate
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/** Utility for reading configuration from a Map[String,String].
 * Collects errors for missing or malformed values so callers can
 * report all issues at once. Optional configuration fields are not
 * supported; missing keys will result in a failure.
 */
class MapConfigReader(map: Map[String, String]) {
  private val errors = ListBuffer.empty[String]

  def getString(key: String): Option[String] = map.get(key) match {
    case Some(v) => Some(v)
    case None =>
      errors += s"$key does not exist"
      None
  }

  def getInt(key: String): Option[Int] = map.get(key) match {
    case Some(v) =>
      Try(v.toInt).toOption match {
        case Some(i) => Some(i)
        case None =>
          errors += s"$key is expecting Int type"
          None
      }
    case None =>
      errors += s"$key does not exist"
      None
  }

  def getDouble(key: String): Option[Double] = map.get(key) match {
    case Some(v) =>
      Try(v.toDouble).toOption match {
        case Some(d) => Some(d)
        case None =>
          errors += s"$key is expecting Double type"
          None
      }
    case None =>
      errors += s"$key does not exist"
      None
  }

  def getLong(key: String): Option[Long] = map.get(key) match {
    case Some(v) =>
      Try(v.toLong).toOption match {
        case Some(l) => Some(l)
        case None =>
          errors += s"$key is expecting Long type"
          None
      }
    case None =>
      errors += s"$key does not exist"
      None
  }

  def getBoolean(key: String): Option[Boolean] = map.get(key) match {
    case Some(v) =>
      Try(v.toBoolean).toOption match {
        case Some(b) => Some(b)
        case None =>
          errors += s"$key is expecting Boolean type"
          None
      }
    case None =>
      errors += s"$key does not exist"
      None
  }

  def getDate(key: String): Option[LocalDate] = map.get(key) match {
    case Some(v) =>
      Try(LocalDate.parse(v)).toOption match {
        case Some(d) => Some(d)
        case None =>
          errors += s"$key is expecting ISO date"
          None
      }
    case None =>
      errors += s"$key does not exist"
      None
  }

  /** Print any collected errors and throw IllegalArgumentException if present. */
  private def assertValid(): Unit = {
    if (errors.nonEmpty) {
      println("Invalid config values:")
      errors.foreach(e => println(s"- $e"))
      throw new IllegalArgumentException("Invalid configuration")
    }
  }

  /**
   * Construct an instance of the given case class using reflection. Field names
   * must match configuration keys exactly and supported types are String,
   * Int, Long, Double, Boolean and java.time.LocalDate.
   */
  def as[T: TypeTag : ClassTag]: T = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val tpe = typeOf[T]
    val classSymbol = tpe.typeSymbol.asClass
    val classMirror = mirror.reflectClass(classSymbol)
    val ctor = tpe.decl(termNames.CONSTRUCTOR).asMethod
    val params = ctor.paramLists.flatten

    val values = params.map { p =>
      val name = p.name.toString
      val t = p.typeSignature
      val opt =
        if (t =:= typeOf[String]) getString(name)
        else if (t =:= typeOf[Int]) getInt(name)
        else if (t =:= typeOf[Double]) getDouble(name)
        else if (t =:= typeOf[Long]) getLong(name)
        else if (t =:= typeOf[Boolean]) getBoolean(name)
        else if (t =:= typeOf[LocalDate]) getDate(name)
        else {
          errors += s"$name has unsupported type $t"
          None
        }
      opt
    }

    assertValid()

    val ctorMirror = classMirror.reflectConstructor(ctor)
    ctorMirror(values.map(_.get): _*).asInstanceOf[T]
  }
}

object MapConfigReader {
  def apply(map: Map[String, String]): MapConfigReader = new MapConfigReader(map)

  /** Convenience method to directly construct a case class from the map. */
  def read[T: TypeTag : ClassTag](map: Map[String, String]): T =
    MapConfigReader(map).as[T]
}
