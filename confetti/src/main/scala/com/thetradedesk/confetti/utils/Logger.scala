package com.thetradedesk.confetti.utils

/** Simple logging interface allowing different implementations. */
trait Logger {
  def debug(message: String): Unit
  def info(message: String): Unit
  def warn(message: String): Unit
  def error(message: String): Unit
}

/** Factory for creating loggers. */
trait LoggerFactory {
  def getLogger(logGroup: String, logStream: String): Logger
}
