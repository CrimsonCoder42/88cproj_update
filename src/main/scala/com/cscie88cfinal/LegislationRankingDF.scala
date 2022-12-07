package com.cscie88cfinal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object LegislationRankingDF extends App {

  // only needs to show errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // creating a spark session https://sparkbyexamples.com/spark/spark-schema-explained-with-examples/
  val legSpark = SparkSession.builder()
    .appName("Legislation Ranking")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val BillsAndLawsDF = legSpark.read
    .format("csv")
    .option("header", true)
    .load("src/main/resources/data/Bills and laws 1999 to 2022 - 1999to2022.csv")


  // need to split to rows unknown length https://sparkbyexamples.com/spark/spark-split-dataframe-column-into-multiple-columns/
//  val comitteeSplit = BillsAndLawsDF.select(split(col("Committees"), "|").getItem(0).as("House Co"),
//    split(col("Committees"), "|").getItem(1).as("Senate")
//    .drop("name")


  // all needed rankings
  val congressRank = BillsAndLawsDF.groupBy("Congress").count().orderBy(desc("count"))
  val sponsorRank = BillsAndLawsDF.groupBy("Sponsor").count().orderBy(desc("count"))
  val comitteesRank = BillsAndLawsDF.groupBy("Committees").count().orderBy(desc("count"))


// remove all nulls from sponsor and comittee
  sponsorRank.na.drop().show(false)
  comitteesRank.na.drop().show(false)




}
