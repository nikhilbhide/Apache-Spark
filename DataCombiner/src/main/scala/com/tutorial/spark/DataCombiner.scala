package com.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
case class nodeForum(id:String,title:String,	tagnames:String,author_id:String,body:String,node_type:String, parent_id:String,abs_parent_id:String, added_at:String, score:String,	state_string:String,last_edited_id:String, last_activity_by_id:String,	last_activity_at:String,	active_revision_id:String, extra:String,	extra_ref_id:String,	extra_count:String,marked:String)
case class userForum(user_ptr_id:String,reputation:String, gold:String,silver:String,bronze:String)
object DataCombiner {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.
				master("local")
				.appName("spark session example")
				.getOrCreate()

				//set new runtime options
				spark.conf.set("spark.sql.shuffle.partitions", 6)
				val userData = spark.read.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				.option("header",true)
				.load("/home/cloudera/Hadoop/Data/forum_data/forum_users.tsv")
				println("user schema" + userData.printSchema())		
				println("total user records are " + userData.count())
				//read node forum data
				val nodeData = spark.read.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				.option("header",true)
				.load("/home/cloudera/Hadoop/Data/forum_data/forum_node.tsv")

				import spark.implicits._
				val nodeDataDS = nodeData.as[nodeForum]   
				val userDataDS = userData.as[userForum]
	  		println("node schema" + nodeDataDS.printSchema())		
		  	println("total node records are " + nodeData.count())
		  	//join data sets
			  val joinedDS = nodeDataDS.join(userData, nodeDataDS.col("author_id")===userDataDS.col("user_ptr_id"))
				println("joined schema" + joinedDS.printSchema())
				println("total records of joined df are " + joinedDS.count())
	}
}