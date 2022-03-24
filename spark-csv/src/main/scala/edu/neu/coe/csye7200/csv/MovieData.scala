package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class MovieData{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieData")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val path = "spark-csv/src/main/resources/movie_metadata.csv"

  val movieDataFrame = spark.read.option("delimiter", ",")
    .option("header", "true")
    .csv(path)

  def meanMovieRating() = {
    val avge: DataFrame = movieDataFrame.select("movie_title", "imdb_score")
      .groupBy("movie_title")
      .agg(functions.avg("imdb_score"))
    avge.show() // avg for all movies aggregated by name
    val mean = movieDataFrame.select(functions.avg("imdb_score"))
    mean.show() // avg for all movies
    mean.collect()(0).get(0)
  }

  def standardDeviationMovieRating(): Any = {
    val sdv = movieDataFrame.select(functions.stddev("imdb_score"))
    sdv.show()
    sdv.collect()(0).get(0)
  }
}

object MovieDataObj extends MovieData
{
  def main(args:Array[String])
  {
    val data = new MovieData()
    data.meanMovieRating() //result: 6.453200745804848
    data.standardDeviationMovieRating() //result: 0.9988071293753289
  }
}