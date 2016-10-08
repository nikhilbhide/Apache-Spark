package com.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.spark.sql.types.LongType

case class Logs(ip:String,unknown1: String, unknown2:String,timestamp:String, unknown3:String,	url:String,accessCode:String,contentSize:String)

object LogAnalyzer {
	def main(args: Array[String]) {
		//set new runtime options
		val spark = SparkEngine.instance()
		import spark.implicits._
				val input = SparkEngine.instance().read
				.format("com.databricks.spark.csv")				                                          
				.option("delimiter", " ")
				.option("header",true)
				.load("/home/cloudera/Hadoop/Data/LogData/apache-samples/access_log/access_log")

				input.foreach(record=>println(record.getString(7)))
				println("Input count" + input.count)
				val filterdInput = input.filter(record=>util.isNumeric(record.getString(7)))
				.as[Logs]
						println("filteredInput count" + filterdInput.count)
						filterdInput.cache()
						val accessLogsDS = filterdInput.selectExpr("ip", 
								"unknown1", 
								"unknown2", 
								"timestamp", 
								"unknown3",
								"url",
								"accessCode",
								"cast(contentSize as long) contentSize")

								val countByIpAddress = accessLogsDS.agg(min("contentSize").name("minContentSize"),
										max("contentSize").name("maxContentSize"),
										avg("contentSize").name("avgContentSize"))
										countByIpAddress.printSchema()
										countByIpAddress.foreach(row=>println("Min:"+row.get(0)+","+"Max:"+row.get(1)+","+"Avg:"+row.get(2)))
	}
}