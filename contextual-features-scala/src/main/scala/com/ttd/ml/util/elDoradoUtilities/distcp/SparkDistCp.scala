package com.ttd.ml.util.elDoradoUtilities.distcp

/* NOTE:
 * This is a direct copy from the Identity teams neocortex library. At first chance, this should be replaced with
 * a dependency on the library in question.
 *
 * Created by Vardan Tovmasyan on 9/17/2018
 * Copyright (c) 2018 by theTradeDesk. All rights reserved.
 * Used some ideas and code from:
 * https://github.com/hortonworks-spark/cloud-integration/
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.ttd.ml.util.elDoradoUtilities.io.FSUtils
import com.ttd.ml.util.elDoradoUtilities.config.SerializableConfiguration

case class SparkDistCpArgs(srcDir: String,
                           destDir: String,
                           maxDegreeOfParallelism: Int = 10000,
                           deleteSrcOnSuccess: Boolean = false,
                           cleanDestDir: Boolean = false,
                           pathFilter: Option[PathFilter] = None,
                           throwIfSourceEmpty: Boolean = false)

/**
 * Allows distributed directory copy using Spark
 */
object SparkDistCp {
  private val logger = LoggerFactory.getLogger(SparkDistCp.getClass)

  def run(args: SparkDistCpArgs) (implicit spark: SparkSession): List[String] = {

    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    // create a serializable wrapper for hadoop configuration to propagate to worker nodes
    val wrappedHadoopConfig = new SerializableConfiguration(hadoopConfig)

    // make sure the destination directory exists
    FSUtils.ensureDirectoryExists(args.destDir)

    if (checkIfAnyFilesToCopy(args.srcDir, args.destDir, args.throwIfSourceEmpty)) {
      if (args.cleanDestDir) {
        logger.info("----------------------------------------------------------------------------------------------------------------")
        logger.info(s"Smart cleaning destination directory `${args.destDir}` ...")
        logger.info("----------------------------------------------------------------------------------------------------------------")
        cleanTargetLeafSubDirectories(args.srcDir, args.destDir)
        logger.info("----------------------------------------------------------------------------------------------------------------")
      }

      logger.info("Creating copy tasks ...")

      val copyTasks = getCopyTasks(args, hadoopConfig)

      logger.info(s"${copyTasks.length} tasks created!")

      logger.info("Preparing the distributed copy operation ...")

      val copyOperation = new CopyFileTasksRDD(
        spark.sparkContext,
        copyTasks,
        if (copyTasks.length > args.maxDegreeOfParallelism) args.maxDegreeOfParallelism else copyTasks.length,
        taskIndex => copyTasks(taskIndex).blockLocations
      ).map(task => executeCopyTask(task, wrappedHadoopConfig))

      logger.info("Executing the distributed copy operation ...")

      // execute
      copyOperation.collect()

      // clean up source files if asked
      // TODO: explore distributed delete options
      if (args.deleteSrcOnSuccess) {
        logger.info(s"Cleaning the source directory '${args.srcDir}' after successful copy ...")
        // here we may have a race condition when while we were copying more files were added to the source directory
        // this is non supported scenario therefore we will skip handling that for now
        FSUtils.cleanDirectory(args.srcDir)
      }

      // return the list of files written
      copyTasks.map(_.dest)
    } else {
      logger.warn(s"Source directory '${args.srcDir}' is empty - skipping copy")

      // return an empty list - we did nothing
      List()
    }
  }

  private def checkIfAnyFilesToCopy(srcDir: String, destDir: String, throwIfEmpty: Boolean)(implicit spark: SparkSession): Boolean = {
    val subDirs = FSUtils.listDirectories(srcDir)
    val files = FSUtils.listFiles(srcDir).filter(_ != "_SUCCESS")

    if (subDirs.isEmpty && files.isEmpty) {
      if (throwIfEmpty) {
        throw new Exception(s"Attempted dist cp from empty directory '$srcDir' - would have deleted everything in dest '$destDir'")
      }

      false
    } else {
      true
    }
  }

  /**
   * Cleaning leaf target directories before copying the new files there
   *
   * Scenario 1: if the source directory contains just files
   *          we remove everything in the destination directory
   * Scenario 2: if the source directory contains sub-directories
   *          we will recursively go deep until we find directories that contain only files
   *          and clean directories with similar relative paths in the target directory
   *          E.g let's say we are copying
   *          srcDir
   *              |-A
   *              |-B
   *                |-C
   *                |-D
   *                  |-E
   *          we will check and empty the following directories
   *          1. destDir/A
   *          2. destDir/A/B/C
   *          3. destDir/A/B/D/E
   * Note Scenario #1 is covered by #2, so we will have a generic recursive implementation
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @param spark spark session
   */
  private def cleanTargetLeafSubDirectories(srcDir: String, destDir: String)(implicit spark: SparkSession) : Unit = {
    // get source directory sub directories
    val subDirs = FSUtils.listDirectories(srcDir)
    if (subDirs.isEmpty) {
      // leaf directory, clean the corresponding destination directory
      logger.info(s"Cleaning leaf destination directory '$destDir' ...")
      FSUtils.cleanDirectory(destDir)
    } else {
      logger.info(s"Recursively cleaning into non-leaf destination directory '$destDir' ...")
      subDirs.foreach(subDir =>
        cleanTargetLeafSubDirectories(FSUtils.combinePaths(srcDir, subDir), FSUtils.combinePaths(destDir, subDir)))
    }
  }

  private def getCopyTasks(args: SparkDistCpArgs, hadoopConfig: Configuration) : List[CopyTask] = {
    val srcDirPath = new Path(args.srcDir)
    val srcFS = srcDirPath.getFileSystem(hadoopConfig)

    logger.info("----------------------------------------------------------------------------------------------------------------")
    logger.info(s"Source directory: ${srcDirPath.toString}")
    logger.info("----------------------------------------------------------------------------------------------------------------")

    val remoteIterator = RemoteIteratorWrapper(srcFS.listFiles(srcDirPath, true))
    // apply path filter if specified
    val files = args.pathFilter match {
      case Some(filter) => remoteIterator.filter(fileStatus => {
        val accepted = filter.accept(fileStatus.getPath)
        if (!accepted)
          logger.info(s"Source file ${fileStatus.getPath} skipped by filter ${filter}")
        accepted
      })

      case _ => remoteIterator
    }

    val copyTasks = files.map {
      fileStatus =>
        val srcFilePath = fileStatus.getPath.toString
        logger.info(s"Source file: $srcFilePath")
        val filePathPostfix = extractFilePathPostfix(srcFilePath, srcDirPath.toString)
        logger.info(s"File postfix: $filePathPostfix")
        val destFilePath = FSUtils.combinePaths(args.destDir, filePathPostfix)
        logger.info(s"Destination file: $destFilePath")
        val blockLocations = fileStatus.getBlockLocations
          .map(blockLocation => blockLocationToHost(blockLocation))
          .flatMap(_.toSet)
        logger.info("----------------------------------------------------------------------------------------------------------------")
        CopyTask(srcFilePath, destFilePath, fileStatus.getLen, blockLocations)
    }

    scala.util.Random.shuffle(copyTasks).toList
  }

  private def executeCopyTask(task: CopyTask, config: SerializableConfiguration) : Unit = {
    // extract the hadoop configuration
    val hadoopConfig = config.get()

    // compose the source and destination paths
    val srcPath = new Path(task.src)
    val destPath = new Path(task.dest)

    FileUtil.copy(
      srcPath.getFileSystem(hadoopConfig), srcPath,
      destPath.getFileSystem(hadoopConfig), destPath,
      false, true, hadoopConfig)
  }

  /**
   * Pattern that lets extracting actual path from values like
   * hdfs://ip-10-251-119-86.ec2.internal:8020/user/hadoop/avails/2018/09/17/01/_SUCCESS
   * where we only need
   * /user/hadoop/avails/2018/09/17/01/_SUCCESS
   */
  private val hdfsFilePathPattern = """^hdfs\://(?:[\w-\.]+)\:[\d]+(/.+)$""".r
  //private val hdfsFilePathPattern = """"^hdfs\://(?:[\w-\.]+)\:[\d]+(/.+)$\"""".r

  /**
   * Extracts the file path postfix based on the directory path
   * @param path file path
   * @param dir directory path
   * @return file path postfix
   */
  def extractFilePathPostfix(path: String, dir: String) : String = {

    // remove any useless prefixes from directory path
    val clearDir = removePathPrefix(dir)

    // check if we are dealing with HDFS path with host and port
    val pathMatch = hdfsFilePathPattern.findFirstMatchIn(path)
    val actualPath = if (pathMatch.nonEmpty) {
      pathMatch.get.group(1)
    } else {
      path
    }

    // try to find the
    val startPos = actualPath.indexOf(clearDir)

    // sanity check
    if (startPos < 0) throw new RuntimeException(s"File path: '$actualPath' didn't contain the dir path: '$clearDir'")

    actualPath.substring(startPos + clearDir.length)
  }

  private def removePathPrefix(path: String) : String = {
    if (path.startsWith("hdfs:"))
      path.substring(5)
    else
      path
  }

  private def blockLocationToHost(blockLocation: BlockLocation): Seq[String] = {
    blockLocation.getHosts match {
      case null => Nil
      case hosts => hosts.filterNot(_.equals("localhost"))
    }
  }

  private case class RemoteIteratorWrapper(underlying: RemoteIterator[LocatedFileStatus]) extends Iterator[LocatedFileStatus] {
    override def hasNext = underlying.hasNext
    override def next = underlying.next
  }
}

