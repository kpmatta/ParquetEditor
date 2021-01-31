package org.xorbit.spark

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession

trait SparkObj {
  lazy val spark : SparkSession = SparkSession.builder()
    .appName("Parquet-Editor")
    .master("local[*]")
    .config("spark.sql.jsonGenerator.ignoreNullFields", false)
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
}
