package com.example.spark

import org.apache.spark.sql.SparkSession

object ExecuteSQL extends App {

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

  flight.createTempView("flight")

  val result = spark.sql(
    """
      |SELECT DEST_COUNTRY_NAME, sum(count) as sum
      |FROM flight
      |GROUP BY DEST_COUNTRY_NAME
      |SORT BY sum DESC
      |LIMIT 5
      |""".stripMargin)

  result.explain()
  result.show()

  spark.stop()
}
