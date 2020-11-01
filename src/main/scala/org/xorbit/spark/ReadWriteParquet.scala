package org.xorbit.spark

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.File

import org.apache.spark.sql.types.{DataType, StructType}
import org.xorbit.utils.FileUtil
import org.apache.log4j.{Level, Logger}

case class Address(city: String, country: String)
case class Student(id: Int, name : String, grade: Int, address : Address )

object ReadWriteParquet {
  private var schemaIn : Option[StructType] = None
  private var schemaOut : Option[StructType] = None

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ParquetEditor")
    .master("local[1]")
    .config("spark.sql.jsonGenerator.ignoreNullFields", false)
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  spark.sparkContext.setLogLevel("ERROR")

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

  def readParquetFile(parquetPath : String) : Array[String] = {
    val df = spark.read.parquet(parquetPath)
    schemaIn = Some(df.schema)
    df.toJSON.collect()
  }

  def readTextFile(txtFilePath : String, schema: Option[StructType]) : Array[String] = {
    if(schema.isEmpty) {
      throw new IllegalArgumentException("Input Schema missing to load Json File")
    }
    val source = scala.io.Source.fromFile(txtFilePath)
    val lines = source.getLines().toArray
    source.close()
    lines
  }

  def writeTextFile(jsonLines: Array[String], jsonPath : String, schema: StructType):Unit = {
    if(jsonLines.isEmpty) return
    val tmpFile = File.createTempFile("tmp", "json")

    try {
      import spark.implicits._
      val jsonDS = spark.createDataset(jsonLines)
      val updatedJsonLines = spark.read.schema(schema).json(jsonDS).toJSON.collect()

      val writer = new BufferedWriter(new FileWriter(tmpFile.getAbsoluteFile))
      updatedJsonLines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
      writer.flush()
      writer.close()
      FileUtil.moveFile(tmpFile, new File(jsonPath))
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtil.deleteFile(tmpFile)
    }
  }

  def writeParquetFile(jsonLines: Array[String], parquetPath: String, schema: StructType): Unit = {
    import spark.implicits._
    val jsonDS = spark.createDataset(jsonLines)
    spark.read
      .schema(schema)
      .json(jsonDS)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)
  }
}
