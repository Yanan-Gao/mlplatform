package com.thetradedesk.confetti.utils

/**
 * Factory for creating CloudWatchLogger instances for a given
 * log group and log stream. A new logger is created each time.
 */
// The CloudWatchLogger constructor is responsible for verifying and creating
// CloudWatch log groups and streams. The factory simply exposes a convenient
// accessor method.
object CloudWatchLoggerFactory {
  /**
   * Obtain a CloudWatchLogger for the provided log group and stream.
   * A new logger instance is created on every call.
   */
  def getLogger(logGroup: String, logStream: String): CloudWatchLogger =
    CloudWatchLogger(logGroup, logStream)
}
