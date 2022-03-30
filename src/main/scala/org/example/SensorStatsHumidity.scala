package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.reflect.io.File

object SensorStatsHumidity {

  def main(args: Array[String]): Unit ={
    // Uncomment to run with arguments
    //val filePath = args(0)
    val filePath = "/home/tg/IdeaProjects/SensorStatsHumidity/rsc/"
    if (File(filePath).exists) {

    val spark = SparkSession.builder()
      .master("local")
      .appName("HumiditySensorStats")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

      // Reading csv files from the folder and adding them into a dataframe
      val df = spark.read.format("csv")
        .option("inferSchema","true")
        .option("header","True")
        .load(filePath)

      // Filtering sensor-id rows with only NaN values
      val grouped = df.groupBy("sensor-id")
        .agg(min("humidity"), mean("humidity"), max("humidity"))
        .sort(desc("avg(humidity)"))
      val df_NaN = grouped.filter(grouped("min(humidity)").isNaN)
      df_NaN.show(false)

      // Removing NaN values from the aggregation and sorts by descending average value
      val df_notNaN = df.na.drop().groupBy("sensor-id")
        .agg(min("humidity"), mean("humidity"), max("humidity"))
        .sort(desc("avg(humidity)"))

      df_notNaN.show(false)

      // Collecting to List
      val sensor_stats_notNaN = df_notNaN.select(Seq("sensor-id", "min(humidity)", "avg(humidity)", "max(humidity)")
        .map(c => col(c)): _*).collect().toList

      val sensor_stats_NaN = df_NaN.select(Seq("sensor-id", "min(humidity)", "avg(humidity)", "max(humidity)")
        .map(c => col(c)): _*).collect().toList

      //println(sensor_stats_notNaN)
      //println(sensor_stats_NaN)

      // Printing processed statistics
      val PROCESSED_FILES = Option(new java.io.File(filePath).list).map(_.count(_.endsWith(".csv"))).getOrElse(0)
      val FAILED_MEASUREMENTS = df.filter(col("humidity").isNaN).count()
      val PROCESSED_MEASUREMENTS = df.count() - FAILED_MEASUREMENTS

      println("Num of processed files: " + PROCESSED_FILES)
      println("Num of processed measurements: " + PROCESSED_MEASUREMENTS)
      println("Num of failed measurements: " + FAILED_MEASUREMENTS)
      println("\nSensors with highest avg humidity:\n\nsensor-id,min,avg,max")

      for(i <- sensor_stats_notNaN.indices)
        println(
          sensor_stats_notNaN(i).mkString(",")
        )

      for (j <- sensor_stats_NaN.indices)
        println(
          sensor_stats_NaN(j).mkString(",")
        )

    } else
      println("Path not found!")
  }
}
