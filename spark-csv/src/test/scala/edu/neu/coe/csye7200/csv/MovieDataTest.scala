package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow

import scala.io.Source
import scala.util.Try

class MovieDataTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  behavior of "Spark"

  it should "show movie data" taggedAs Slow in {
    val mdy = Try(Source.fromResource("movie_metadata.csv"))
    mdy.isSuccess shouldBe true
  }

  it should "show mean rating for all movies" taggedAs Slow in {
    MovieDataObj.meanMovieRating() should matchPattern {
      case 6.453200745804848=>
    }
  }

  it should "show standard deviation rating for all movies" taggedAs Slow in {
    MovieDataObj.standardDeviationMovieRating() should matchPattern {
      case 0.9988071293753289=>
    }
  }
}
