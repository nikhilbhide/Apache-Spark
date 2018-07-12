package com.nik.spark

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

object KmeansClustering {
	def main(args:Array[String]) {
		import org.apache.log4j._
		Logger.getLogger("org").setLevel(Level.ERROR)

		// Spark Session
		val spark = SparkSession.builder().master("local[*]").getOrCreate()

		// Use Spark to read in the wholesale customers data csv file.
		val dataset = spark.read.option("header","true").option("inferSchema","true").csv("Wholesale customers data.csv")		// Print the Schema of the DataFrame

		//Display data
		val colnames = dataset.columns
		val firstrow = dataset.head(1)(0)
		println("\n")
		println("Example Data Row")
		for(ind <- Range(1,colnames.length)){
			println(colnames(ind))
			println(firstrow(ind))
			println("\n")
		}
		
		import spark.implicits._
		//Select columns
		val feature_data = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicassen")
	
		// Create a new VectorAssembler object called assembler for the feature
		// columns as the input Set the output column to be called features
		val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")

		// Use the assembler object to transform the feature_data
		// Call this new data training_data
		val training_data = assembler.transform(feature_data).select("features")

		// Find suitable value based on WSSE. Here we figured out that suitable value is 9
		val kmeans = new KMeans().setK(9)

		// Fit that model to the training_data
		val model = kmeans.fit(training_data)

		// Evaluate clustering by computing Within Set Sum of Squared Errors.
		val WSSSE = model.computeCost(training_data)
		println(s"Within Set Sum of Squared Errors = $WSSSE")

		// Shows the result.
		println("Cluster Centers: ")
		model.clusterCenters.foreach(println)
		println("Cluster Sizes: ")
    model.summary.clusterSizes.foreach { x => println(x) }
	}
}