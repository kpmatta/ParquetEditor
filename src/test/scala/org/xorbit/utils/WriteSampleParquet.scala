package org.xorbit.utils

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Location(lat: Double, lang: Double)
case class CapitalCity (id:Int, name: String, country: String, location: Location)

@RunWith(classOf[JUnitRunner])
class WriteSampleParquet extends FunSuite{
  lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Testing")
    .getOrCreate()

  ignore("Write Sample Parquet File") {
    val capitalList = List(
      CapitalCity(1, "London", "UK", Location(1.0, 2.0)),
      CapitalCity(2, "New Delhi", "India", Location(2.0, 3.0)),
      CapitalCity(3, "Katmandu", "Nepal", Location(4.4, 5.6)),
      CapitalCity(4, "Paris", "France", Location(3.4, 6.7))
    )

    import spark.implicits._
    spark.createDataset(capitalList)
      .write
      .mode("overwrite")
      .parquet("./output/capitals")
  }
}
