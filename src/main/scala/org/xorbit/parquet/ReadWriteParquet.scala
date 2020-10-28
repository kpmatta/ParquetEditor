package org.xorbit.parquet

import java.io.{BufferedWriter, ByteArrayInputStream, DataInputStream, FileWriter, InputStream}

import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader, ParquetWriter}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.kitesdk.data.spi.JsonUtil
import java.io.File

import org.xorbit.utils.FileUtil

case class Address(city: String, country: String)
case class Student(id: Int, name : String, grade: Int, address : Address )

object ReadWriteParquet {

  def WriteSampleParquet(): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[1]").getOrCreate()

    val students = List(
      Student(1, "Jack", 9, Address("New York", "USA")),
      Student(2, "Tom", 3, Address("Chicago", "USA")),
      Student(5, "Dan", 6, Address("London", "UK"))
    )

    val df = spark.createDataFrame(students)
    df.write.mode(SaveMode.Overwrite).parquet("./input/parquet/")
  }

  def convertParquetToJson(parquetPath: String, jsonFilePath: String): Unit = {
    val jsonArray: Array[String] = readParquetFile(parquetPath)
    if (jsonArray.nonEmpty) {
      writeTextFile(jsonArray, jsonFilePath)
    }
  }

  def convertJsonToParquet(jsonFilePath: String, parquetPath: String): Unit = {
    val jsonLines = readTextFile(jsonFilePath)
    if(jsonLines.nonEmpty) {
      writeParquetFile(jsonLines, parquetPath)
    }
  }

  def readParquetFile(parquetPath: String): Array[String] = {
    val reader: ParquetReader[GenericRecord] = AvroParquetReader
      .builder[GenericRecord](new Path(parquetPath))
      .build
    Iterator.continually(reader.read())
      .takeWhile(_ != null)
      .map(_.toString)
      .toArray
  }

  def readTextFile(txtFilePath : String) : Array[String] = {
    val source = scala.io.Source.fromFile(txtFilePath)
    val lines = source.getLines().toArray
    source.close()
    lines
  }

  def writeTextFile(jsonLines: Array[String], jsonPath : String):Unit = {
    if(jsonLines.isEmpty) return
    val tmpFile = File.createTempFile("tmp", "json")

    try {
      val writer = new BufferedWriter(new FileWriter(tmpFile.getAbsoluteFile))
      jsonLines.foreach { line =>
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

  def writeParquetFile(jsonLines: Array[String], parquetPath: String): Unit = {
    if (jsonLines.isEmpty) return

    val tmpFile = File.createTempFile("tmp", "parquet")

    try {
      val jsonNode = JsonUtil.parse(jsonLines.head)
      val schema = JsonUtil.inferSchema(jsonNode, "GenericSchema")

      val writer: ParquetWriter[GenericRecord] = AvroParquetWriter
        .builder[GenericRecord](new Path(tmpFile.getAbsolutePath))
        .withSchema(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withDataModel(GenericData.get)
        .build

      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
      jsonLines.foreach { json =>
        val input: InputStream = new ByteArrayInputStream(json.getBytes)
        val din: DataInputStream = new DataInputStream(input)
        val decoder: Decoder = DecoderFactory.get.jsonDecoder(schema, din)
        val datum: GenericRecord = reader.read(null, decoder)
        writer.write(datum)
      }
      writer.close()
      FileUtil.moveFile(tmpFile, new File(parquetPath))
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtil.deleteFile(tmpFile)
    }
  }
}
