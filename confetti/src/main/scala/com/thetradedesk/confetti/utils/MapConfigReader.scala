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
class MapConfigReader(map: Map[String, String], logger: Logger) {
  private val errors = ListBuffer.empty[String]

  private def getSeqOpt[T](
      key: String,
      parse: String => T,
      typeName: String): Option[Seq[T]] =
    map.get(key) match {
      case Some(v) =>
        Try(v.split(",").filter(_.nonEmpty).map(s => parse(s.trim)).toSeq).toOption match {
          case Some(seq) => Some(seq)
          case None =>
            errors += s"$key cannot be parsed as Seq[$typeName]"
            None
        }
      case None => None
    }

  private def getSeq[T](
      key: String,
      parse: String => T,
      typeName: String): Option[Seq[T]] =
    map.get(key) match {
      case Some(v) =>
        Try(v.split(",").filter(_.nonEmpty).map(s => parse(s.trim)).toSeq).toOption match {
          case Some(seq) => Some(seq)
          case None =>
            errors += s"$key cannot be parsed as Seq[$typeName]"
            None
        }
      case None =>
        errors += s"$key does not exist"
        None
    }

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

  def getFloatOpt(key: String): Option[Float] = map.get(key) match {
    case Some(v) =>
      Try(v.toFloat).toOption match {
        case Some(f) => Some(f)
        case None =>
          errors += s"$key is expecting Float type"
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

  def getStringSeqOpt(key: String): Option[Seq[String]] =
    getSeqOpt(key, identity, "String")

  def getIntSeqOpt(key: String): Option[Seq[Int]] =
    getSeqOpt(key, _.toInt, "Int")

  def getLongSeqOpt(key: String): Option[Seq[Long]] =
    getSeqOpt(key, _.toLong, "Long")

  def getDoubleSeqOpt(key: String): Option[Seq[Double]] =
    getSeqOpt(key, _.toDouble, "Double")

  def getFloatSeqOpt(key: String): Option[Seq[Float]] =
    getSeqOpt(key, _.toFloat, "Float")

  def getDateSeqOpt(key: String): Option[Seq[LocalDate]] =
    getSeqOpt(key, LocalDate.parse, "LocalDate")

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

  def getFloat(key: String): Option[Float] = map.get(key) match {
    case Some(v) =>
      Try(v.toFloat).toOption match {
        case Some(f) => Some(f)
        case None =>
          errors += s"$key is expecting Float type"
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

  def getStringSeq(key: String): Option[Seq[String]] =
    getSeq(key, identity, "String")

  def getIntSeq(key: String): Option[Seq[Int]] =
    getSeq(key, _.toInt, "Int")

  def getLongSeq(key: String): Option[Seq[Long]] =
    getSeq(key, _.toLong, "Long")

  def getDoubleSeq(key: String): Option[Seq[Double]] =
    getSeq(key, _.toDouble, "Double")

  def getFloatSeq(key: String): Option[Seq[Float]] =
    getSeq(key, _.toFloat, "Float")

  def getDateSeq(key: String): Option[Seq[LocalDate]] =
    getSeq(key, LocalDate.parse, "LocalDate")

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
   * Int, Long, Double, Float, Boolean, java.time.LocalDate and various Seq types.
   * Fields wrapped in
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
            else if (inner =:= typeOf[Float]) getFloatOpt(name)
            else if (inner =:= typeOf[Long]) getLongOpt(name)
            else if (inner =:= typeOf[Boolean]) getBooleanOpt(name)
            else if (inner =:= typeOf[LocalDate]) getDateOpt(name)
            else if (inner =:= typeOf[Seq[String]]) getStringSeqOpt(name)
            else if (inner =:= typeOf[Seq[Int]]) getIntSeqOpt(name)
            else if (inner =:= typeOf[Seq[Long]]) getLongSeqOpt(name)
            else if (inner =:= typeOf[Seq[Double]]) getDoubleSeqOpt(name)
            else if (inner =:= typeOf[Seq[Float]]) getFloatSeqOpt(name)
            else if (inner =:= typeOf[Seq[LocalDate]]) getDateSeqOpt(name)
            else {
              errors += s"$name has unsupported type $t"
              None
            }
          Some(value)
        } else if (t =:= typeOf[String]) getString(name)
        else if (t =:= typeOf[Int]) getInt(name)
        else if (t =:= typeOf[Double]) getDouble(name)
        else if (t =:= typeOf[Float]) getFloat(name)
        else if (t =:= typeOf[Long]) getLong(name)
        else if (t =:= typeOf[Boolean]) getBoolean(name)
        else if (t =:= typeOf[LocalDate]) getDate(name)
        else if (t =:= typeOf[Seq[String]]) getStringSeq(name)
        else if (t =:= typeOf[Seq[Int]]) getIntSeq(name)
        else if (t =:= typeOf[Seq[Long]]) getLongSeq(name)
        else if (t =:= typeOf[Seq[Double]]) getDoubleSeq(name)
        else if (t =:= typeOf[Seq[Float]]) getFloatSeq(name)
        else if (t =:= typeOf[Seq[LocalDate]]) getDateSeq(name)
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
