package com.cscie88cfinal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MoreDF extends App {

  // only needs to show errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // creating a spark session https://sparkbyexamples.com/spark/spark-schema-explained-with-examples/
  val spark = SparkSession.builder()
    .appName("Legislation Ranking")
    .config("spark.master", "local")
    .getOrCreate()




}
