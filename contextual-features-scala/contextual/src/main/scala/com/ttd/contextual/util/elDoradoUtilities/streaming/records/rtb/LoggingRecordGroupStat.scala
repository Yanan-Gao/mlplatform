package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

case class LoggingRecordGroupStat(RecordGroupId: Option[String], ApplicationType: Option[String], RecordGroupStatus: Option[String], RecordDurationMs: Long, TaskDurationMs: Long, TotalRecordCountInGroup: Int)
