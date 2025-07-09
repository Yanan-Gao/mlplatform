package com.thetradedesk.confetti.utils

import java.time.LocalDate
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

/** Utility for reading configuration from a Map[String,String].
 * Collects errors for missing or malformed values so callers can
 * report all issues at once. Fields typed as `Option[T]` are treated
 * as optional; other fields are required.
 */
class MapConfigReader(map: Map[String, String], logger: CloudWatchLogger) {
  private val errors = ListBuffer.empty[String]

  def getString(key: String): Option[String] = map.get(key) match {
    case Some(v) => Some(v)
    case None =>
      errors += s"$key does not exist"
      None
  }

  // Optional variants that do not treat missing keys as an error
  def getStringOpt(key: String): Option[String] = map.get(key)

  def getIntOpt(key: String): Option[Int] = map.get(key) match {
    case Some(v) =>
      Try(v.toInt).toOption match {
        case Some(i) => Some(i)
        case None =>
          errors += s"$key is expecting Int type"
          None
      }
    case None => None
  }

  def getDoubleOpt(key: String): Option[Double] = map.get(key) match {
    case Some(v) =>
      Try(v.toDouble).toOption match {
        case Some(d) => Some(d)
        case None =>
          errors += s"$key is expecting Double type"
          None
      }
    case None => None
  }

  def getLongOpt(key: String): Option[Long] = map.get(key) match {
    case Some(v) =>
      Try(v.toLong).toOption match {
        case Some(l) => Some(l)
        case None =>
          errors += s"$key is expecting Long type"
          None
      }
    case None => None
  }

  def getBooleanOpt(key: String): Option[Boolean] = map.get(key) match {
    case Some(v) =>
      Try(v.toBoolean).toOption match {
        case Some(b) => Some(b)
        case None =>
          errors += s"$key is expecting Boolean type"
          None
      }
    case None => None
  }

  def getDateOpt(key: String): Option[LocalDate] = map.get(key) match {
    case Some(v) =>
      Try(LocalDate.parse(v)).toOption match {
        case Some(d) => Some(d)
        case None =>
          errors += s"$key is expecting ISO date"
          None
      }
    case None => None
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
      val message = ("Invalid config values:" +: errors.map(e => s"- $e")).mkString("\n")
      logger.error(message)
      throw new IllegalArgumentException(message)
    }
  }

  /**
   * Construct an instance of the given case class using reflection. Field names
   * must match configuration keys exactly and supported types are String,
   * Int, Long, Double, Boolean and java.time.LocalDate. Fields wrapped in
   * `Option` are treated as optional.
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
        if (t <:< typeOf[Option[_]]) {
          val inner = t.typeArgs.head
          val value =
            if (inner =:= typeOf[String]) getStringOpt(name)
            else if (inner =:= typeOf[Int]) getIntOpt(name)
            else if (inner =:= typeOf[Double]) getDoubleOpt(name)
            else if (inner =:= typeOf[Long]) getLongOpt(name)
            else if (inner =:= typeOf[Boolean]) getBooleanOpt(name)
            else if (inner =:= typeOf[LocalDate]) getDateOpt(name)
            else {
              errors += s"$name has unsupported type $t"
              None
            }
          Some(value)
        } else if (t =:= typeOf[String]) getString(name)
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
