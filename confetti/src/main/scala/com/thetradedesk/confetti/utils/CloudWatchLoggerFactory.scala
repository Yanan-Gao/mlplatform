package com.thetradedesk.confetti.utils

/**
 * Factory for creating CloudWatchLogger instances for a given
 * log group and log stream. A new logger is created each time.
 */
import com.amazonaws.regions.Regions
import com.amazonaws.services.logs.AWSLogsClientBuilder
import com.amazonaws.services.logs.model._

import scala.collection.JavaConverters._

object CloudWatchLoggerFactory {
  // single AWSLogs client shared for factory operations
  private val client = AWSLogsClientBuilder.standard()
    .withRegion(Regions.US_EAST_1)
    .build()

  /**
   * Ensure the log group exists, creating it if necessary.
   */
  private def ensureLogGroup(group: String): Unit = {
    val existing = client.describeLogGroups(new DescribeLogGroupsRequest()
      .withLogGroupNamePrefix(group)).getLogGroups.asScala
    if (!existing.exists(_.getLogGroupName == group)) {
      client.createLogGroup(new CreateLogGroupRequest(group))
    }
  }

  /**
   * Ensure the log stream exists within the log group, creating it if needed.
   */
  private def ensureLogStream(group: String, stream: String): Unit = {
    val existing = client.describeLogStreams(new DescribeLogStreamsRequest(group)
      .withLogStreamNamePrefix(stream)).getLogStreams.asScala
    if (!existing.exists(_.getLogStreamName == stream)) {
      client.createLogStream(new CreateLogStreamRequest(group, stream))
    }
  }

  /**
   * Obtain a CloudWatchLogger for the provided log group and stream.
   * A new logger instance is created on every call. The log group and
   * stream will be created if they do not already exist.
   */
  def getLogger(logGroup: String, logStream: String): CloudWatchLogger = {
    ensureLogGroup(logGroup)
    ensureLogStream(logGroup, logStream)
    new CloudWatchLogger(logGroup, logStream)
  }
}
