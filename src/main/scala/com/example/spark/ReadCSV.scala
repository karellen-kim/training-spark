package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object ReadCSV extends App {

  val spark = SparkSession.builder()
    .appName("hello world")
    .config("spark.master", "local")
    .getOrCreate()

  val path = getClass.getResource("/csv").toURI.getPath

  val flight = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path)
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "sum")
    .sort(desc("sum"))

  flight.explain()
  flight.show(5)

  spark.stop()
}
