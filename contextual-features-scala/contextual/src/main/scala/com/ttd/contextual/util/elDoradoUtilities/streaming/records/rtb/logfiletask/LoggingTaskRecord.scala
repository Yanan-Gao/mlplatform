/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb.logfiletask

/**
 * @param GroupId 
 * @param Application 
 * @param GroupStatus 
 * @param GroupStartTime Minimum log start time of the file
 * @param GroupEndTime Maximum log start time of the file
 * @param TaskStartTime Processing start time of the log file
 * @param TaskEndTime Processing end time of the log file
 * @param SuccessfulRecordCount 
 * @param FailedRecordCount 
 * @param FailedLines 
 */
case class LoggingTaskRecord(GroupId: String, Application: String, GroupStatus: String, GroupStartTime: java.sql.Timestamp, GroupEndTime: java.sql.Timestamp, TaskStartTime: java.sql.Timestamp, TaskEndTime: java.sql.Timestamp, SuccessfulRecordCount: Int, FailedRecordCount: Int, FailedLines: Seq[Int])