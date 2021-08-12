package com.ttd.ml.util.elDoradoUtilities.config

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/**
 * Configuration wrapper that let's sending configs to Spark worker nodes
 * @param conf
 */
class SerializableConfiguration(var conf: Configuration) extends Serializable {
  /**
   * Default constructor
   */
  def this() {
    this(new Configuration())
  }

  /**
   * Gets the wrapped configuration
   * @return
   */
  def get(): Configuration = conf

  /**
   * Writes the wrapped configuration to output stream
   * @param out
   */
  private def writeObject (out: ObjectOutputStream): Unit = {
    conf.write(out)
  }

  /**
   * Reads the configuration from the input stream
   * @param in
   */
  private def readObject (in: ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  /**
   * Reads the configuration without data fields
   */
  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}
