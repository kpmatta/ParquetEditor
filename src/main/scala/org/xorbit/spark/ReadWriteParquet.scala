package org.xorbit.spark

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.File
import java.nio.file.Files
import org.xorbit.utils.ResourceHandler._

import org.apache.spark.sql.types.{DataType, StructType}
import org.xorbit.utils.FileUtility
import org.apache.log4j.{Level, Logger}

object ReadWriteParquet {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ParquetEditor")
    .master("local[*]")
    .config("spark.sql.jsonGenerator.ignoreNullFields", value = false)
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")

  def readSchema(schemaFilePath : String): Option[StructType] = {
    using(scala.io.Source.fromFile(schemaFilePath)) { bs =>
      val jsonStr = bs.mkString
      val schema = DataType.fromJson(jsonStr).asInstanceOf[StructType]
      Option(schema)
    }
  }

  def writeSchema(schema: StructType, filePathName : String): Unit = {
    val schemaLines = schema.prettyJson.split("\\R")
    writeTextFile(schemaLines.toList, filePathName)
  }

  def readParquetFile(parquetPath : String): (List[String], StructType) = {
    val df = spark.read.parquet(parquetPath)
    (df.toJSON.collect().toList,
      df.schema)
  }

  def readTextFile(txtFilePath : String): List[String] = {
    using(scala.io.Source.fromFile(txtFilePath)) {
      f => f.getLines().toList
    }
  }

  def writeTextFile(lines: List[String], filePath: String): Unit = {
    using(new BufferedWriter(new FileWriter(filePath))) { writer =>
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
      writer.flush()
    }
  }

  def writeJsonFile(jsonLines: List[String],
                    jsonPath : String,
                    schema: StructType):Unit = {
    if(jsonLines.isEmpty) return
    val tmpFile = File.createTempFile("tmp", ".json")

    try {
      import spark.implicits._
      val jsonDS = spark.createDataset(jsonLines)
      val updatedJsonLines = spark.read
        .schema(schema)
        .option("mode", "FAILFAST")
        .json(jsonDS).toJSON
        .collect().toList

      writeTextFile(updatedJsonLines, tmpFile.getAbsolutePath)
      FileUtility.moveFile(tmpFile.getAbsolutePath, jsonPath)
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFile.getAbsolutePath)
    }
  }

  def writeParquetFile(jsonLines: List[String],
                       parquetPath: String,
                       schema: StructType): Unit = {
    if(jsonLines.isEmpty) return

    import spark.implicits._
    val jsonDS = spark.createDataset(jsonLines)
    val tmpFolder = Files.createTempDirectory("tmp")
    val tmpFilePath = tmpFolder.toFile.getAbsolutePath

    try {
      val df = spark.read
        .schema(schema)
        .option("mode", "FAILFAST")
        .json(jsonDS)

      df.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(tmpFilePath)

      FileUtility.moveFile(tmpFilePath, parquetPath)
    }
    catch {
      case ex:Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFilePath)
    }
  }
}
