package com.cscie88cfinal

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// ranks all Politicians by how many bills they sponsored
object SponsorRankings {

  final case class Politician(Sponsor: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Sponsor Rankings")
      .master("local[*]")
      .getOrCreate()

    // Create schema
    val politicalSchema = new StructType()
      .add("LegislationNum", StringType, nullable = true)
      .add("Title", StringType, nullable = true)
      .add("Sponsor", StringType, nullable = true)
      .add("DOI", StringType, nullable = true)

    import spark.implicits._

    // Load up dataset
    val sponsorDS = spark.read
      .option("sep", ",")
      .schema(politicalSchema)
      .csv("src/main/resources/data/LegislationAndSponsors.csv")
      .as[Politician]

    val sponsorRank = sponsorDS.groupBy("Sponsor").count().orderBy(desc("count"))

    sponsorRank.show()

    spark.stop()
  }

}

// 3:15:22:23 filtering a string

// 3:15:28:31 cosponsors vs subject https://en.wikipedia.org/wiki/Pearson_correlation_coefficient


// 3:15:6:37

//// Booleans
//val dramaFilter = col ("Major_Genre") equalTo "Drama" // instead of === you can use equalTo
//val goodRatingFilter = col ("IMDB_Rating") > 7.0
//val preferredFilter = dramaFilter and goodRatingFilter
//
//moviesDF.select ("Title").where (dramaFilter)
//// + multiple ways of filtering
