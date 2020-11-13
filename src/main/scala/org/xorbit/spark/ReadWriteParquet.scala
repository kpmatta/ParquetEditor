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
  private var schemaIn : Option[StructType] = None
  private var schemaOut : Option[StructType] = None

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ParquetEditor")
    .master("local[*]")
    .config("spark.sql.jsonGenerator.ignoreNullFields", value = false)
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")

  def cleanUp(): Unit = {
    schemaIn = None
    schemaOut = None
  }

  def getSchemaIn: Option[StructType] = {
    schemaIn
  }

  def getSchemaOut: Option[StructType] = {
    Option(schemaOut.getOrElse(schemaIn.get))
  }

  def readSchema(schemaFilePath : String): Option[StructType] = {
    val bs = scala.io.Source.fromFile(schemaFilePath)
    val jsonStr = bs.mkString
    val schema = DataType.fromJson(jsonStr).asInstanceOf[StructType]
    bs.close()
    Option(schema)
  }

  def readInputSchema(schemaPath : String): Unit = {
    schemaIn = readSchema(schemaPath)
  }

  def readOutputSchema(schemaPath : String) : Unit = {
    schemaOut = readSchema(schemaPath)
  }

  def writeSchema(schema: StructType, filePathName : String): Unit = {
    val writer = new BufferedWriter(new FileWriter(filePathName))
    writer.write(schema.prettyJson)
    writer.flush()
    writer.close()
  }

  def readParquetFile(parquetPath : String): List[String] = {
    val df = spark.read.parquet(parquetPath)
    schemaIn = Some(df.schema)
    df.toJSON.collect().toList
  }

  def readTextFile(txtFilePath : String): List[String] = {
    using(scala.io.Source.fromFile(txtFilePath)) {
      f => f.getLines().toList
    }
  }

  def writeTextFile(jsonLines: Array[String], jsonPath : String, schema: StructType):Unit = {
    if(jsonLines.isEmpty) return
    val tmpFile = File.createTempFile("tmp", ".json")

    try {
      import spark.implicits._
      val jsonDS = spark.createDataset(jsonLines)
      val updatedJsonLines = spark.read
        .schema(schema)
        .option("mode", "FAILFAST")
        .json(jsonDS)
        .toJSON
        .collect()

      val writer = new BufferedWriter(new FileWriter(tmpFile.getAbsoluteFile))
      updatedJsonLines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
      writer.flush()
      writer.close()
      FileUtility.moveFile(tmpFile.getAbsolutePath, jsonPath)
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFile.getAbsolutePath)
    }
  }

  def writeParquetFile(jsonLines: Array[String], parquetPath: String, schema: StructType): Unit = {
    import spark.implicits._
    val jsonDS = spark.createDataset(jsonLines)

    val tmpFolder = Files.createTempDirectory("tmp")
    val tmpFilePath = tmpFolder.toFile.getAbsolutePath

    try {
      val df = spark.read
        .schema(schema)
        .option("mode", "FAILFAST")
        .json(jsonDS)

      if (!df.isEmpty) {
        df.write
          .mode(SaveMode.Overwrite)
          .parquet(tmpFilePath)

        FileUtility.moveFile(tmpFilePath, parquetPath)
      }
    }
    catch {
      case ex:Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFilePath)
    }
  }
}
