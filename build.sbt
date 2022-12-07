ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "final_project_88c"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.apache.spark" %% "spark-mllib" % "3.2.1",
  "org.apache.spark" %% "spark-streaming" % "3.2.1",
  "com.databricks" %% "spark-xml" % "0.15.0",
  "com.github.pureconfig" %% "pureconfig" % "0.17.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

)