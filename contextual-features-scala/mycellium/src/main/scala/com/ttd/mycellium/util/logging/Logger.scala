package com.ttd.mycellium.util.logging

import org.apache.log4j

trait Logger {
  @transient lazy val log: log4j.Logger = org.apache.log4j.Logger.getLogger(getClass.getName)
}
