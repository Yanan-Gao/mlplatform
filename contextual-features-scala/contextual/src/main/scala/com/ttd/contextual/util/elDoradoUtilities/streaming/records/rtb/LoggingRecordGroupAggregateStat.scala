package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

case class LoggingRecordGroupAggregateStat(AvgTaskDurationMs: Double, MinTaskDurationMs: Long, MaxTaskDurationMs: Long, AvgGroupDurationMs: Double, MinGroupDurationMs: Long, MaxGroupDurationMs: Long, AvgTotalRecordsPerGroup: Double, MinTotalRecordsPerGroup: Long, MaxTotalRecordsPerGroup: Long, TotalGroupCount: Int, RecordGroupStatCollection: Seq[LoggingRecordGroupStat])
