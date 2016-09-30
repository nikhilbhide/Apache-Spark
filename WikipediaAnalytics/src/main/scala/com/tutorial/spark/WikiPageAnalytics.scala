package com.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class Edit(project:String,pageTitle:String,numRequests:Long)
case class EditEntry(project:String,pageTitle:String,numRequests:Long)
class Parser extends Serializable {

	//function to parse lines of wikipedia document
	def parseLine(line:String) = {
		line.split("""\s+""") match {
		case Array(project,page,numRequests,contentSize)=>Some(EditEntry(project,page,numRequests.toLong))
		case _ => None
		}
	}
}

object WikiPediaAnalytics {
	def main(args: Array[String]) {
		//create spark configuration
		val sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local")
				//create sparik conctext
				val sparkContext = new SparkContext(sparkConf)
		//create rdd of line read from wiki pages 
		val rddWikiPage = sparkContext.textFile("/home/cloudera/Hadoop/Data/WikiPedia/pagecounts-20160101-000000 (1).gz")
		rddWikiPage.take(100).foreach (println)
		rddWikiPage.cache()
		println("size of document is : " + rddWikiPage.count())

		//initialize spark sql context
		val sqlcontext = new org.apache.spark.sql.SQLContext(sparkContext)
		val sqlContext1 = new SQLContext(sparkContext)

		import sqlcontext.implicits._

		//convert rdd to data frame
		val wordCountDF = rddWikiPage.flatMap ((new Parser).parseLine)
		.toDF()
		//convert data frame to data set		                            
		val wordCountDS = wordCountDF.as[Edit]
		println(wordCountDS.count())
		//find top 100 pages by hits
		wordCountDS.filter(e=>(e.project=="en") && (!e.pageTitle.contains(":")) && (!e.pageTitle.startsWith(".")))
	          	 .groupBy("pageTitle")
				       .agg(sum("numRequests").name("totalRequests"))
				       .sort(desc("totalRequests"))
				       .take(100)
				       .foreach{e=>println(e(0) + "-"+e(1))
		}
	}
}