package com.thetradedesk.confetti.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.logs.AWSLogsClientBuilder
import com.amazonaws.services.logs.model._

import scala.collection.JavaConverters._

/**
  * Simple logger that writes messages to AWS CloudWatch Logs.
  *
  * @param logGroup  name of the CloudWatch log group
  * @param logStream name of the CloudWatch log stream
  */
class CloudWatchLogger(logGroup: String, logStream: String) {
  // Import members from the companion object so we can access the shared
  // AWSLogs client and helper functions without qualifying them.
  import CloudWatchLogger._

  // ensure the log group and stream exist on instantiation
  ensureLogGroup(logGroup)
  ensureLogStream(logGroup, logStream)

  // Cache the next sequence token so we don't have to fetch it from CloudWatch
  // for every log entry. It is refreshed after each putLogEvents call.
  private var sequenceToken: Option[String] = getSequenceToken(logGroup, logStream)

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

// Companion object for CloudWatchLogger. In Scala, objects hold the equivalent
// of static fields and methods. We keep the AWSLogs client here so all logger
// instances share a single client.
object CloudWatchLogger {
  private val client = AWSLogsClientBuilder.standard()
    .withRegion(Regions.US_EAST_1)
    .build()

  private def ensureLogGroup(group: String): Unit = {
    val existing = client.describeLogGroups(new DescribeLogGroupsRequest()
      .withLogGroupNamePrefix(group)).getLogGroups.asScala
    if (!existing.exists(_.getLogGroupName == group)) {
      client.createLogGroup(new CreateLogGroupRequest(group))
    }
  }

  private def ensureLogStream(group: String, stream: String): Unit = {
    val existing = client.describeLogStreams(new DescribeLogStreamsRequest(group)
      .withLogStreamNamePrefix(stream)).getLogStreams.asScala
    if (!existing.exists(_.getLogStreamName == stream)) {
      client.createLogStream(new CreateLogStreamRequest(group, stream))
    }
  }

  private def getSequenceToken(group: String, stream: String): Option[String] = {
    val streams = client.describeLogStreams(new DescribeLogStreamsRequest(group)
      .withLogStreamNamePrefix(stream)).getLogStreams.asScala
    streams.headOption.flatMap(s => Option(s.getUploadSequenceToken))
  }

  def apply(logGroup: String, logStream: String): CloudWatchLogger =
    new CloudWatchLogger(logGroup, logStream)
}
