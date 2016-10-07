package com.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import scala.collection.Parallelizable

object PICalculator {
  
  def getRandomNumber() : Integer = {
     val x = Math.random()
    val y = Math.random()
    if (x*x + y*y < 1) 1 else 0
  }
	def main(args: Array[String]) {
		val spark = SparkSession.builder.
				master("local")
				.appName("spark session example")
				.getOrCreate()
		val noOfSamples = 100000000
		val count = spark.sparkContext.parallelize(1 to noOfSamples)
		                  .map(i=> getRandomNumber())
		                  .reduce(_+_)
		println("value of pi is"+4.0 * count / noOfSamples);
	}
}