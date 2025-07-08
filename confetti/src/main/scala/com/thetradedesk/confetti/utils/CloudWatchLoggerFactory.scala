package com.thetradedesk.confetti.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.logs.{AWSLogs, AWSLogsClientBuilder}
import com.amazonaws.services.logs.model._

import scala.collection.JavaConverters._

/**
 * Factory for creating CloudWatchLogger instances for a given
 * log group and log stream. A new logger is created each time.
 * The factory ensures the log group and stream exist before
 * returning the logger.
 */
object CloudWatchLoggerFactory {
  /**
   * Obtain a CloudWatchLogger for the provided log group and stream.
   * A new logger instance is created on every call.
   */
  def getLogger(logGroup: String, logStream: String): CloudWatchLogger = {
    val client = AWSLogsClientBuilder.standard()
      .withRegion(Regions.US_EAST_1)
      .build()

    ensureLogGroup(client, logGroup)
    ensureLogStream(client, logGroup, logStream)

    new CloudWatchLogger(client, logGroup, logStream)
  }

  private def ensureLogGroup(client: AWSLogs, group: String): Unit = {
    val existing = client.describeLogGroups(new DescribeLogGroupsRequest()
      .withLogGroupNamePrefix(group)).getLogGroups.asScala
    if (!existing.exists(_.getLogGroupName == group)) {
      client.createLogGroup(new CreateLogGroupRequest(group))
    }
  }

  private def ensureLogStream(client: AWSLogs, group: String, stream: String): Unit = {
    val existing = client.describeLogStreams(new DescribeLogStreamsRequest(group)
      .withLogStreamNamePrefix(stream)).getLogStreams.asScala
    if (!existing.exists(_.getLogStreamName == stream)) {
      client.createLogStream(new CreateLogStreamRequest(group, stream))
    }
  }
}
