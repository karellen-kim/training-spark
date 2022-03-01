package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

object SimpleML extends App {

  val spark = SparkSession.builder()
    .appName("simpleml")
    .config("spark.master", "local")
    .getOrCreate()

  val path = getClass.getResource("/retail-data/by-day").toURI.getPath

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"${path}/*.csv")

  val preppedDF = df
    .na.fill(0)
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)

  val trainDF = preppedDF.where("InvoiceDate < '2011-07-01'")
  val testDF = preppedDF.where("InvoiceDate >= '2011-07-01'")

  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  val encoder = new OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded")

  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
    .setOutputCol("features")

  val transformationPipeline = new Pipeline()
    .setStages(Array(indexer, encoder, vectorAssembler))

  val fittedPipeline = transformationPipeline.fit(trainDF)

  val transformedTraining = fittedPipeline.transform(trainDF)
  transformedTraining.cache()

  val kmeans = new KMeans().setK(2).setSeed(1L)
  val model = kmeans.fit(transformedTraining)
  val predictions = model.transform(transformedTraining)

  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predictions)
  println(s"Silhouette with squared euclidean distance = $silhouette")
  println("Cluster Centers: ")
  model.clusterCenters.foreach(println)

}
