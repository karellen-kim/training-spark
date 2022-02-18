package com.example.spark

import org.apache.spark.sql.SparkSession

object Remote extends App {

  val spark = SparkSession
    .builder
    .appName("hello world")
    .config("spark.master", "local")
    .getOrCreate

  // 실행 계획
  val df = spark.range(1000).toDF("number")

  // 실제 연산 수행
  println(df.count())

  spark.stop()
}
