package com.thetradedesk.confetti.utils

import org.apache.log4j.{AppenderSkeleton, Level, LoggingEvent}

/**
 * Log4j appender that writes log events to AWS CloudWatch using CloudWatchLogger.
 *
 * @param logGroup name of the CloudWatch log group
 * @param logStream name of the log stream
 */
class CloudWatchAppender(val logGroup: String, val logStream: String) extends AppenderSkeleton {
  private lazy val logger = CloudWatchLoggerFactory.getLogger(logGroup, logStream)

  override def append(event: LoggingEvent): Unit = {
    val message = if (layout != null) layout.format(event) else event.getRenderedMessage
    event.getLevel match {
      case Level.DEBUG => logger.debug(message)
      case Level.INFO  => logger.info(message)
      case Level.WARN  => logger.warn(message)
      case Level.ERROR => logger.error(message)
      case _           => logger.info(message)
    }
  }

  override def close(): Unit = {}

  override def requiresLayout(): Boolean = true
}
