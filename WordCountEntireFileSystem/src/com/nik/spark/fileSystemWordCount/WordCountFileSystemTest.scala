package com.nik.spark.fileSystemWordCount

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, Row, SQLContext, DataFrameReader}

/**
 * Calculates word count for entire file path under process. 
 */
object WordCountFileSystemTest {
  val DefaultHdfsPath = "/home/"
  val DefaultRecursiveScan = false
  
 /**
 * Accepts file path as an input and flag, which indicates whether scan file system recursively or not.
 *  
 * @param args Input arguments, first argument is assumed(if provided) to be file path and second argument (if provided) is assumed to be recursiveScan flag indicator.  
 */
  def main(args:Array[String]) {
    val traverseHDFSInstance = new WordCountFileSystem()
    traverseHDFSInstance.initSpark()
    val filePaths = new ListBuffer[String]() 
    var filePathToTraverse = DefaultHdfsPath
    var recrusveScanIndicator = DefaultRecursiveScan
    if(args!=null && args.length>0) {
      filePathToTraverse = args(0)
      if(args.length==2) 
        recrusveScanIndicator = args(1).toBoolean
    }
    
    traverseHDFSInstance.traverseDirectory(filePathToTraverse, recrusveScanIndicator, filePaths)
    traverseHDFSInstance.findWordCount(filePaths)
  } 
}