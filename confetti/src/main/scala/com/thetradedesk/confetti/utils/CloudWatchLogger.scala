package com.thetradedesk.confetti.utils

import com.amazonaws.services.logs.AWSLogs
import com.amazonaws.services.logs.model._

import scala.collection.JavaConverters._

/**
 * Simple logger that writes messages to AWS CloudWatch Logs.
 *
 * @param logGroup  name of the CloudWatch log group
 * @param logStream name of the CloudWatch log stream
 */
class CloudWatchLogger(client: AWSLogs, logGroup: String, logStream: String) {
  // Cache the next sequence token so we don't have to fetch it from CloudWatch
  // for every log entry. It is refreshed after each putLogEvents call.
  private var sequenceToken: Option[String] = fetchSequenceToken()

  private def fetchSequenceToken(): Option[String] = {
    val streams = client.describeLogStreams(new DescribeLogStreamsRequest(logGroup)
      .withLogStreamNamePrefix(logStream)).getLogStreams.asScala
    streams.headOption.flatMap(s => Option(s.getUploadSequenceToken))
  }

  def debug(message: String): Unit = log("DEBUG", message)

  def info(message: String): Unit = log("INFO", message)

  def warn(message: String): Unit = log("WARN", message)

  def error(message: String): Unit = log("ERROR", message)

  def log(level: String, message: String): Unit = {
    val event = new InputLogEvent()
      .withTimestamp(System.currentTimeMillis())
      .withMessage(s"[$level] $message")

    val request = new PutLogEventsRequest()
      .withLogGroupName(logGroup)
      .withLogStreamName(logStream)
      .withLogEvents(java.util.Arrays.asList(event))

    // Attach the cached sequence token if available. If CloudWatch returns a
    // next token we update our cache for subsequent log calls.
    sequenceToken.foreach(request.setSequenceToken)
    val result = client.putLogEvents(request)
    sequenceToken = Option(result.getNextSequenceToken)
  }
}

