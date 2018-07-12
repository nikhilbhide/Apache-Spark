package com.tutorial.spark

import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._  

object PurchaseSalesByCategory { 
   def main(args: Array[String]) { 
      val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map()) 
		  val input = sc.textFile("in.txt") 
      val count = input.flatMap(line ⇒ line.split(" ")) 
      .map(word ⇒ (word, 1)) 
      .reduceByKey((o1,o2)=>o1+o2)
      count.saveAsTextFile("outfile2") 
      System.out.println("OK"); 
   } 
} 