package com.thetradedesk.confetti.utils

/**
 * Factory for creating CloudWatchLogger instances for a given
 * log group and log stream. A new logger is created each time.
 */
object CloudWatchLoggerFactory {
  /**
   * Obtain a CloudWatchLogger for the provided log group and stream.
   * A new logger instance is created on every call.
   */
  def getLogger(logGroup: String, logStream: String): CloudWatchLogger =
    new CloudWatchLogger(logGroup, logStream)
}
