package com.ttd.ml.util.elDoradoUtilities.io

/* NOTE:
 * This is a direct copy from the Identity teams neocortex library. At first chance, this should be replaced with
 * a dependency on the library in question.
 */

import java.io.{File, FileInputStream, IOException, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, LocalDate}

/**
 * Exposes file system related utility functions
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * !! THESE METHODS ARE INTENDED TO BE CALLED FROM THE SPARK DRIVER. CALLING FROM WORKER NODES WILL MOST LIKELY NOT WORK
 * !! AS HADOOP CONFIGURATION IS NOT PROPAGATED. IF ABSOLUTELY NEEDED WE WILL HAVE TO MAKE SOME CHANGES HERE
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 */
object FSUtils {
  /**
   * Checks if the given path is a local file (or directory) path
   * @param path first path part
   * @return true if the path represents a local path, otherwise - false
   */
  def isLocalPath(path: String) (implicit spark: SparkSession) : Boolean = {
    !(path.startsWith("hdfs://") || path.startsWith("s3://") || path.startsWith("s3n://") || path.startsWith("s3a://") || path.startsWith("file://"))
  }

  /**
   * Combines 2 path strings
   * @param path1 first path part
   * @param path2 second path part
   * @return combined path string
   */
  def combinePaths(path1: String, path2: String, delimiter: String = "/") : String = {
    val normalizedPath1 = if(path1.endsWith(delimiter)) path1 else path1 + delimiter
    val normalizedPath2 = if(path2.startsWith(delimiter)) path2.substring(1) else path2
    normalizedPath1 + normalizedPath2
  }

  /**
   * Checks if a valid data set output exists in the given path
   * @param path data set path
   * @param spark spark session
   * @return true if a "loadable" data set exists in the given location, otherwise - false
   */
  def checkValidDataSetOutputExists(path: String)
                                   (implicit spark: SparkSession) : Boolean = {
    // make sure the _SUCCESS file exists
    fileExists(combinePaths(path, "_SUCCESS"))
  }

  /**
   * Checks if the given file exists
   * @param file file path
   * @param spark spark session
   * @return true if the file exists, otherwise - false
   */
  def fileExists(file: String)
                (implicit spark: SparkSession) : Boolean = {
    if (isLocalPath(file)) {
      // handle local files without using spark context
      val f = new File(file)
      if (!f.exists) false
      else {
        if (!f.isFile)
          throw new IllegalArgumentException(file + " is not a file")
        true // file exists
      }
    } else {
      val fileSystem = getFileSystem(file)
      val path = new Path(file)
      if (!fileSystem.exists(path)) false
      else {
        // make sure it is a file
        if (!fileSystem.isFile(path))
          throw new IllegalArgumentException(file + " is not a file")
        true // file exists
      }
    }
  }

  /**
   * Checks if the given directory exists
   * @param directory directory path
   * @param spark spark session
   * @return true if the file exists, otherwise - false
   */
  def directoryExists(directory: String)
                     (implicit spark: SparkSession) : Boolean = {
    if (isLocalPath(directory)) {
      val d = new File(directory)
      if (!d.exists) false
      else {
        if (!d.isDirectory) throw new IllegalArgumentException(directory + " is not a directory")
        true
      }
    } else {
      val fileSystem = getFileSystem(directory)
      val path = new Path(directory)

      if (!fileSystem.exists(path)) false
      else {
        // make sure it is a directory
        if (!fileSystem.isDirectory(path)) throw new IllegalArgumentException(directory + " is not a directory")

        true // directory exists
      }
    }
  }

  /**
   * Ensures that the given directory exists
   * @param directory directory path
   * @param spark spark session
   * @return true if operation was successful, otherwise - false
   */
  def ensureDirectoryExists(directory: String)
                           (implicit spark: SparkSession) : Boolean = {
    if (isLocalPath(directory)) {
      val d = new File(directory)
      if (d.exists){
        if(!d.isDirectory) throw new IllegalArgumentException(directory + " exists, nut not a directory!")
        true // ensured
      } else {
        d.mkdirs() // try to make directories, won't fail if any of it already exists
      }
    } else {
      getFileSystem(directory).mkdirs(new Path(directory))
    }
  }

  /**
   * Ensures that the given directory exists and is clean
   *
   * @param directory directory path
   * @param spark spark session
   *
   * @return true if operation was successful, otherwise - false
   */
  def ensureCleanDirectory(directory: String) (implicit spark: SparkSession) : Boolean = {
    if (ensureDirectoryExists(directory)) {
      cleanDirectory(directory)
      true
    } else {
      false
    }
  }

  /**
   * Lists the contents of the directory with the given path
   * @param directory directory path
   * @param spark spark session
   * @return array of file status objects
   */
  def listDirectoryContents(directory: String)
                           (implicit spark: SparkSession) : Array[FileStatus] = {
    val fileSystem = getFileSystem(directory)
    val path = new Path(directory)
    if (!fileSystem.exists(path)) {
      // return empty array
      new Array[FileStatus](0)
    } else {
      if (!fileSystem.isDirectory(path))
        throw new IllegalArgumentException(directory + " is not a directory")

      // list the entries
      fileSystem.listStatus(path)
    }
  }

  /**
   * Lists the directory names in the given path
   * @param directory directory path
   * @param spark spark session
   * @return sequence of directory names
   */
  def listDirectories(directory: String)
                     (implicit spark: SparkSession) : Seq[String] = {
    listDirectoryContents(directory).filter(_.isDirectory).map(_.getPath.getName).toSeq
  }

  /**
   * Lists the file names in the given path
   * @param directory directory path
   * @param recursive value indicating whether sub directories should be scanned too
   * @param spark spark session
   * @return sequence of file names
   */
  def listFiles(directory: String, recursive: Boolean = false)
               (implicit spark: SparkSession) : Seq[String] = {
    val dirContents = listDirectoryContents(directory)
    val files = dirContents.filter(_.isFile).map(_.getPath.getName)
    if (recursive) {
      dirContents.filter(_.isDirectory)
        .flatMap(subDir => listFiles(subDir.getPath.toString, recursive).map(f => s"${subDir.getPath.getName}/$f"))
        .union(files)
        .toSeq
    } else {
      // just the files
      files.toSeq
    }
  }


  /**
   * Write bytes to a given file (can be local, hdfs, or S3)
   *
   * @param file path to the file
   * @param data string to write
   * @throws IOException if I/O errors occur
   */
  def writeBytesToFile(file: String, data: Array[Byte])
                      (implicit spark: SparkSession): Unit = {
    // please note that writing to s3:// won't create files in the right folder structure
    // for more info see here: https://notes.mindprince.in/2014/08/01/difference-between-s3-block-and-s3-native-filesystem-on-hadoop.html
    val fileSystem = getFileSystem(file)
    val path = new Path(file)

    // make sure the parent directory exists
    val parentDirectory = path.getParent
    if (!fileSystem.mkdirs(parentDirectory)) {
      throw new IOException(s"The parent directory '${parentDirectory.toString}' couldn't be created!")
    }

    // create a temp file and write the data to it
    var outputFileStream : FSDataOutputStream = null
    try {
      // disable checksum generation
      fileSystem.setWriteChecksum(false)
      fileSystem.setVerifyChecksum(false)
      // create a writable file
      outputFileStream = fileSystem.create(path)
      // write the string as UTF encoded bytes
      outputFileStream.write(data)
    } finally {
      if(outputFileStream != null) {
        // close the file
        outputFileStream.close()
      }
    }
  }

  /**
   * Write a string to the given file (can be local, hdfs or S3)
   *
   * @param file path to the file
   * @param data string to write
   * @throws IOException if I/O errors occur
   */
  def writeStringToFile(file: String, data: String)
                       (implicit spark: SparkSession) : Unit  = {

    writeBytesToFile(file, data.getBytes("UTF-8"))
  }

  /**
   * Reads a string from the given text file (can be local, hdfs or S3)
   *
   * @param file path to the file
   * @return read string
   */
  def readStringFromFile(file: String)
                        (implicit spark: SparkSession) : String = {
    var inputStream : FSDataInputStream = null
    try {
      inputStream = getFileSystem(file).open(new Path(file))
      // read bytes
      val bytes = IOUtils.toByteArray(inputStream)
      // create UTF string
      new String(bytes, StandardCharsets.UTF_8)
    } finally {
      if(inputStream != null)
        inputStream.close()
    }
  }

  /**
   * Gets the size of a given file
   *
   * @param file path to the file
   * @return file size in bytes
   */
  def getFileSize(file: String)
                 (implicit spark: SparkSession) : Long = {
    getFileSystem(file).getFileStatus(new Path(file)).getLen
  }

  /**
   * renames the file. This is untested for renaming across different filesystems (HDFS to s3, for example)
   * @param srcFile source file path
   * @param destFile destination file path
   * @param spark spark session
   * @throws IllegalArgumentException source file doesn't exist or is not a file
   * @throws IOException move operation fails
   */
  def renameFile(srcFile: String, destFile: String)
                (implicit spark: SparkSession) : Unit = {

    val srcFileSystem = getFileSystem(srcFile)
    val srcFilePath = new Path(srcFile)

    // check if file exists
    if (!srcFileSystem.exists(srcFilePath))
      throw new IllegalArgumentException(s"File '$srcFile' doesn't exist!")

    // ensure the source is a file
    if (!srcFileSystem.isFile(srcFilePath))
      throw new IllegalArgumentException(s"The given path '$srcFile' is not a file!")

    srcFileSystem.rename(srcFilePath, new Path(destFile))
  }

  /**
   * Copies the file from one location to another (could be local, HDFS or s3)
   * @param srcFile source file path
   * @param destFile destination file path
   * @param overwrite indicates whether to overwrite the destination file if it already exists
   * @param deleteSrcOnSuccess indicates whether to delete the source after successfully copying to destination
   * @param spark spark session
   * @return true if the file was copied, otherwise - false
   * @throws IllegalArgumentException source file doesn't exist or is not a file
   * @throws IOException copy operation fails
   */
  def copyFile(srcFile: String, destFile: String, overwrite: Boolean = true, deleteSrcOnSuccess: Boolean = false)
              (implicit spark: SparkSession) : Boolean = {

    val srcFileSystem = getFileSystem(srcFile)
    val srcFilePath = new Path(srcFile)

    // check if file exists
    if (!srcFileSystem.exists(srcFilePath))
      throw new IllegalArgumentException(s"File '$srcFile' doesn't exist!")

    // ensure the source is a file
    if (!srcFileSystem.isFile(srcFilePath))
      throw new IllegalArgumentException(s"The given path '$srcFile' is not a file!")

    // copy the file
    FileUtil.copy(
      srcFileSystem,
      srcFilePath,
      getFileSystem(destFile),
      new Path(destFile),
      deleteSrcOnSuccess,
      overwrite,
      spark.sparkContext.hadoopConfiguration)
  }

  /**
   * Copies all files from source directory to destination optionally clearing the destination
   * @param srcDir source directory path
   * @param destDir destination folder path
   * @param deleteSrcOnSuccess value indicating if the source directory needs to be deleted when all files are successfully copied to destination
   * @param cleanDestDir value indicating if the destination directory needs to be cleaned before starting the copy
   * @throws IOException when copy fails
   */
  def copyDirectoryLocal(srcDir: String, destDir: String, deleteSrcOnSuccess: Boolean, cleanDestDir: Boolean = true) : Unit = {

    val dest = new File(destDir)

    if (cleanDestDir) {
      // clean up the destination directory
      FileUtils.cleanDirectory(dest)
    }

    if(deleteSrcOnSuccess) {
      // delete the destination
      FileUtils.deleteDirectory(dest)
      // move the directory
      FileUtils.moveDirectory(new File(srcDir), dest)
    } else {
      // copy the directory contents
      FileUtils.copyDirectory(new File(srcDir), dest)
    }
  }

  /**
   * Copies the directory contents to another location
   * @param srcDir source directory
   * @param destDir destination directory
   * @param deleteSrcOnSuccess value indicating if the source directory needs to be deleted when all files are successfully copied to destination
   * @param cleanDestDir value indicating if the destination directory needs to be cleaned before starting the copy
   * @param spark spark session
   * @note if anything goes wrong during the copy destination folder may end up with inconsistent files
   */
  def copyDirectory(srcDir: String, destDir: String, deleteSrcOnSuccess: Boolean, cleanDestDir: Boolean)
                   (implicit spark: SparkSession) : Unit = {

    // make sure the destination directory exists
    ensureDirectoryExists(destDir)

    if (isLocalPath(srcDir) && isLocalPath(destDir)) {
      copyDirectoryLocal(srcDir, destDir, deleteSrcOnSuccess, cleanDestDir)
    } else {
      if (cleanDestDir) {
        cleanDirectory(destDir)
      }

      for (entry <- listDirectoryContents(srcDir)) {
        val srcEntry = entry.getPath.toString
        val destEntry = combinePaths(destDir, entry.getPath.getName)
        if (entry.isDirectory) {
          // recursively copy the directory
          copyDirectory(srcEntry, destEntry, deleteSrcOnSuccess = false, cleanDestDir = false)
        } else {
          // overwrite
          copyFile(srcEntry, destEntry)
        }
      }

      if (deleteSrcOnSuccess) {
        cleanDirectory(srcDir)
      }
    }
  }

  /**
   * Deletes the given file
   *
   * @param file file path (can be local, HDFS or s3)
   * @param spark spark session
   * @return true if delete is successful else false.
   * @throws IllegalArgumentException invalid path is given
   * @throws IOException deletion fails
   */
  def deleteFile(file: String)
                (implicit spark: SparkSession) : Boolean = {
    val fileSystem = getFileSystem(file)
    val path = new Path(file)
    if (fileSystem.exists(path) && !fileSystem.isFile(path))
      throw new IllegalArgumentException(s"The given path '$file' doesn't represent a file!")
    fileSystem.delete(path, false)
  }

  /**
   * Clears all files and directories in the given folder
   *
   * @param directory directory path (can be local, HDFS or s3)
   * @param spark spark session
   * @throws IllegalArgumentException invalid path is given
   * @throws IOException deletion fails
   */
  def cleanDirectory(directory: String)
                    (implicit spark: SparkSession) : Unit = {
    if (isLocalPath(directory)) {
      FileUtils.cleanDirectory(new File(directory))
    } else {
      listDirectoryContents(directory).foreach(f => {
        // recursively delete the
        if (f.isDirectory) deleteDirectory(f.getPath.toString, recursive = true)
        else deleteFile(f.getPath.toString)
      })
    }
  }

  /**
   * Deletes all files and directories in the given folder
   *
   * @param directory directory path (can be local, HDFS or s3)
   * @param spark spark session
   * @return true if delete is successful else false
   * @throws IllegalArgumentException invalid path is given
   * @throws IOException deletion fails
   */
  def deleteDirectory(directory: String, recursive: Boolean)
                     (implicit spark: SparkSession) : Boolean = {
    if (isLocalPath(directory)) {
      new scala.reflect.io.Directory(new File(directory)).deleteRecursively()
    } else {
      val fileSystem = getFileSystem(directory)
      val path = new Path(directory)
      if (fileSystem.exists(path) && !fileSystem.isDirectory(path))
        throw new IllegalArgumentException(s"The given path '$directory' doesn't represent a directory!")
      fileSystem.delete(path, recursive)
    }
  }

  /**
   * Open the file and gets the read stream
   * @param file s3 file path
   * @param spark spark session
   * @return input stream of bytes for the given file
   * @note the caller code should close the stream after finishing the read operation
   */
  def getFileStream(file: String) (implicit spark: SparkSession) : InputStream = {
    if (isLocalPath(file)) {
      new FileInputStream(file)
    } else {
      getFileSystem(file).open(new Path(file))
    }
  }

  /**
   * Open the gzip file and return the read stream
   * @param file s3 file path
   * @param spark spark session
   * @return input stream of bytes for the given gzip file
   * @note the caller code should close the stream after finishing the read operation
   */
  def getGzipFileStream(file: String) (implicit spark: SparkSession) : GZIPInputStream = {
    new GZIPInputStream(getFileStream(file))
  }

  private def getFileSystem(path: String) (implicit spark: SparkSession) : FileSystem  = {
    if (spark == null) throw new IllegalArgumentException("Spark session can't be null!")
    if (spark.sparkContext == null) throw new IllegalArgumentException("Spark context can't be null!")
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    if (hadoopConfig == null) throw new IllegalArgumentException("Hadoop configuration can't be null!")
    if (isLocalPath(path)) {
      FileSystem.getLocal(hadoopConfig)
    } else {
      FileSystem.get(new URI(path), hadoopConfig)
    }
  }

  // Common helper functions related to parsing paths
  def trimEndSlash(path: String): String = { if (path.endsWith("/")) path.dropRight(1) else path }
  def defaultPathToDateTime(path: String): DateTime = DateTime.parse(path.takeRight(10))
  def defaultPathFilter(path: String): Boolean = path.matches(".*\\d{4}-\\d{2}-\\d{2}$")
  // Lets you customize the success file based on the root directly, specifically because sometimes the success file is in the root folder instead of the dated folder
  def defaultSuccessPathGenerator : (String, String) => String = (graphFolder: String, successFile: String) => s"${graphFolder.split("/").last}/$successFile"

  /**
   * Finds the latest date sub folder within the defined lookback window with a success file.
   * @param rootPath The path to search.
   * @param pathFilter Function to filter out sub folders that are not dates.
   * @param pathToDateTime Function to convert the folder name to a date.
   * @param lookbackWindowInDays The maximum number of dayes ago to consider date folders valid.
   * @return Optional Date and Path for the most recent date folder in the lookback window.
   */
  def getLatestInputPath(
                          rootPath: String,
                          pathFilter: String => Boolean = defaultPathFilter,
                          pathToDateTime: String => DateTime = defaultPathToDateTime,
                          lookbackWindowInDays: Int,
                          successFile: String = "_SUCCESS",
                          successPathGenerator: (String, String) => String = defaultSuccessPathGenerator,
                          throwIfNoMatch: Boolean = false)
                        (implicit spark: SparkSession): Option[Tuple2[DateTime, String]] = {
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
    val inputPaths = getLatestInputPaths(rootPath, pathFilter, pathToDateTime, lookbackWindowInDays, maxNumberOfPaths = 1, successFile, successPathGenerator, throwIfNoMatch )
    val inputDateAndPath = inputPaths.maxBy(_._1)
    return Option(inputDateAndPath)
  }

  /**
   * Finds the latest date sub folder within the defined lookback window with a success file.
   * @param rootPath The path to search.
   * @param pathFilter Function to filter out sub folders that are not dates.
   * @param pathToDateTime Function to convert the folder name to a date.
   * @param lookbackWindowInDays The maximum number of dayes ago to consider date folders valid.
   * @return Optional Date and Path for the most recent date folder in the lookback window.
   */
  def getLatestInputPaths(
                           rootPath: String,
                           pathFilter: String => Boolean = defaultPathFilter,
                           pathToDateTime: String => DateTime = defaultPathToDateTime,
                           lookbackWindowInDays: Int,
                           maxNumberOfPaths: Int = 3,
                           successFile: String = "_SUCCESS",
                           successPathGenerator: (String, String) => String = defaultSuccessPathGenerator,
                           throwIfNoMatch: Boolean = false)
                         (implicit spark: SparkSession): List[Tuple2[DateTime, String]] = {
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
    val minDate = DateTime.now.minusDays(lookbackWindowInDays)

    println(s"Find latest date folder at ${rootPath} since ${minDate}")

    // Get the latest date and folder for latest date sub folder in the lookback window with a matching success file.
    val inputDateToPathMap = FSUtils.listDirectories(rootPath)
      .filter(x => pathFilter(x))
      .map(folder => (pathToDateTime(folder), s"$rootPath/$folder"))
      .filter(x => {
        println(s"Checking ${x._1}, ${x._2}");
        println(s"$rootPath/${successPathGenerator(x._2, successFile)}");
        x._1.compareTo(minDate) >= 0 && x._1.compareTo(DateTime.now) <= 0 && FSUtils.fileExists(s"$rootPath/${successPathGenerator(x._2, successFile)}")
      })

    // Return if nothing found.
    if(inputDateToPathMap.isEmpty) {
      val message = s"Could not find any date sub folders at ${rootPath} in the lookback window that match the date filter and have a matching success file."
      println(message)
      if(throwIfNoMatch)
        throw new Exception(message)
      return List()
    }

    // Return the most recent date and folder.
    val inputDateAndPaths = inputDateToPathMap.sortBy(_._1).take(maxNumberOfPaths)
    return inputDateAndPaths.toList
  }

  def getLatestPathBefore(path: String, date: String, lookbackWindow: Int)(implicit spark: SparkSession): String = {
    val upperDate = LocalDate.parse(date)
    var i = 1
    var find = false
    var pathFound = ""
    while (i <= lookbackWindow && !find) {
      val curDate = upperDate.minusDays(i)
      pathFound = s"${path}/${curDate}"
      if (FSUtils.directoryExists(pathFound))
        find = true
      i += 1
    }
    if (!find)
      throw new RuntimeException(s"didn't find latestPath within ${lookbackWindow} days for ${path}")

    println(pathFound)
    pathFound
  }
}

