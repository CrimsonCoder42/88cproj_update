package com.cscie88cfinal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FilterByDF extends App {

  // only needs to show errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // creating a spark session https://sparkbyexamples.com/spark/spark-schema-explained-with-examples/
  val spark = SparkSession.builder()
    .appName("Legislation Ranking")
    .config("spark.master", "local")
    .getOrCreate()

  val legislationDF = spark.read
    .format("csv")
    .option("header", true)
    .load("src/main/resources/data/Bills and laws 1999 to 2022 - 1999to2022.csv")

  legislationDF.filter(col("Legislation Number").contains("S. ")).show()



}
