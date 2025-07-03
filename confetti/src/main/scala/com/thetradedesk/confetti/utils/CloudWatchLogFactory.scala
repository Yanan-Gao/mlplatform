package com.thetradedesk.confetti.utils

import org.apache.log4j.{Logger, PatternLayout}

/**
 * Factory for obtaining log4j loggers that log to CloudWatch.
 */
object CloudWatchLogFactory {
  /**
   * Returns a logger for the given class and log group. A CloudWatch appender is
   * attached using the class name as the log stream.
   */
  def getLogger(clazz: Class[_], logGroup: String): Logger = {
    val logger = Logger.getLogger(clazz)
    val appenderName = s"CloudWatch-${logGroup}-${clazz.getSimpleName}"
    if (logger.getAppender(appenderName) == null) {
      val appender = new CloudWatchAppender(logGroup, clazz.getSimpleName)
      appender.setName(appenderName)
      appender.setLayout(new PatternLayout("%d{ISO8601} %-5p %c - %m%n"))
      logger.addAppender(appender)
    }
    logger
  }
}
