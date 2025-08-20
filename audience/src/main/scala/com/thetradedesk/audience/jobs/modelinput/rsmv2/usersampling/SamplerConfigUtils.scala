package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

/** Utility helpers for extracting strongly typed values from a configuration
  * object whose concrete type is unknown at compile time. The configuration may
  * either be a case class with accessor methods or a `Map[String, Any]`.
  */
private[usersampling] object SamplerConfigUtils {

  /** Attempt to read a String field from the configuration. */
  def getString(conf: Any, field: String): Option[String] = conf match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].get(field).collect { case s: String => s }
    case _ =>
      try {
        Option(conf.getClass.getMethod(field).invoke(conf): Any).collect { case s: String => s }
      } catch {
        case _: Throwable => None
      }
  }

  /** Attempt to read an Int field from the configuration. */
  def getInt(conf: Any, field: String): Option[Int] = conf match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].get(field).collect { case i: Int => i }
    case _ =>
      try {
        Option(conf.getClass.getMethod(field).invoke(conf): Any).collect { case i: Int => i }
      } catch {
        case _: Throwable => None
      }
  }

  /** Attempt to read a sequence of Int from the configuration. */
  def getSeqInt(conf: Any, field: String): Option[Seq[Int]] = conf match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].get(field).map {
        case seq: Seq[_] => seq.collect { case i: Int => i }
        case other       => throw new IllegalArgumentException(s"Field '$field' expected Seq[Int] but found ${other.getClass}")
      }
    case _ =>
      try {
        Option(conf.getClass.getMethod(field).invoke(conf): Any).map {
          case seq: Seq[_] => seq.collect { case i: Int => i }
          case other       => throw new IllegalArgumentException(s"Field '$field' expected Seq[Int] but found ${other.getClass}")
        }
      } catch {
        case _: Throwable => None
      }
  }
}

