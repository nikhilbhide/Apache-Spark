package com.nik.spark.fileSystemWordCount

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import  org.apache.hadoop.fs.{FileSystem,Path}
import scala.collection.Parallel

/**
 * Traverses file system and finds word count for each of the text file. After that, each RDD results are aggregated to find word count of entire file path under process.
 */

class WordCountFileSystem extends java.io.Serializable {
var sparkContext:org.apache.spark.SparkContext = null

/**
 * Initialize spark session
 */
def initSpark() {
	sparkContext = new SparkContext("local[*]", "TraverseHDFS")   
}
	
/**
 * Traverse file system starting from from input path and filter files based on criteria and return a list of all filepaths.
 * If recursive traverse is true then it scans through the sub-directories and get the file paths.
 *  
 * @param filePath The file path to be traversed 
 * @param recursiveTraverse The indicator which indicates recursive     
 * @param filePaths The list of all file paths in the directory and its subdirectory and so on.
 */
def traverseDirectory(filePath:String,recursiveTraverse:Boolean,filePaths:ListBuffer[String]) {
	val files = FileSystem.get( sparkContext.hadoopConfiguration ).listStatus(new Path(filePath))
			files.foreach { fileStatus => {
				if(!fileStatus.isDirectory() && fileStatus.getPath().getName().endsWith(".txt")) {				  
					filePaths+=fileStatus.getPath().toString()      
				}
				else if(fileStatus.isDirectory()) {
					traverseDirectory(fileStatus.getPath().toString(), recursiveTraverse, filePaths)
				}
			}
	}	
}

/**
 * Read all text files and then create RDD for each one of the file. For each file, word count is figured out and results are added up in the result RDD.
 * Word count is executed on the result RDD as w1ll, which in turn gives word count for all files under provided file path.
 * 
 * @param filePaths The list of all file paths in the directory and its subdirectory and so on.
 * 
 * 
 */
def findWordCount(filePaths:ListBuffer[String]) {
	type pairRDD = (String,Int)
			var resultRDD = sparkContext.emptyRDD[pairRDD]
					val filePathsRDD = sparkContext.parallelize(filePaths.toSeq)
					filePaths.foreach(filePath => {
						val fileRDD = sparkContext.textFile(filePath)
								val resultWordCount = fileRDD.flatMap(record=>record.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>x+y)
								resultWordCount.cache()
								resultRDD = resultRDD.union(resultWordCount)
					})
					val resultWordCountRDDFilePath = resultRDD.reduceByKey((x,y)=>x+y)
					resultWordCountRDDFilePath.foreach{case(word,count) => {
						println(s"Word is ${word} and count is ${count}")
					}
	}
}
}