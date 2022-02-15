package com.ttd.contextual.util.elDoradoUtilities.spark.listener

import org.apache.spark.executor.OutputMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerTaskEnd}

class WriteListener extends SparkListener {
  var rowsWritten: Long = 0

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    Some(taskEnd.taskMetrics.outputMetrics) match {
      case Some(metrics) =>
        rowsWritten += metrics.recordsWritten
    }
  }
}
