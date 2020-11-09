package org.xorbit.parquet_avro

/*
import java.io.{BufferedWriter, ByteArrayInputStream, DataInputStream, File, FileWriter, InputStream}

import org.apache.arrow.vector.ipc.JsonFileReader
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader, ParquetWriter}
import org.xorbit.utils.FileUtility

object PAEditor {
  private var schemaIn : Schema = _
  private var schemaOut : Schema = _

  def main(args: Array[String]): Unit = {
    val path = "/Users/krishnamatta/Krishna/Projects/github/ParquetEditor/input/parquet/part-00000-b0355fd4-824c-4402-8d91-0ae1f9db19ea-c000.snappy.parquet"
    val lines = readParquetFile(path)
    lines.foreach( line => {
      println(line)
    })

    writeSchema(schemaIn, "/Users/krishnamatta/Krishna/Projects/github/ParquetEditor/input/parquet/schema.avsc")
  }

  def getSchemaIn: Schema = {
    schemaIn
  }

  def getSchemaOut: Schema = {
    if(schemaOut == null) schemaIn else schemaOut
  }

  def readSchema(schemaFilePath : String): Schema = {
    val schema = new Schema.Parser().parse(new File(schemaFilePath))
    schema
  }

  def readInputSchema(schemaPath : String): Unit = {
    schemaIn = readSchema(schemaPath)
  }

  def readOutputSchema(schemaPath : String) : Unit = {
    schemaOut = readSchema(schemaPath)
  }

  def writeSchema(schema: Schema, filePathName : String): Unit = {
    val writer = new BufferedWriter(new FileWriter(filePathName))
    writer.write(schema.toString(true))
    writer.flush()
    writer.close()
  }

  def readParquetFile(parquetPath : String) : Array[String] = {
    val reader: ParquetReader[GenericRecord] = AvroParquetReader
      .builder[GenericRecord](new Path(parquetPath))
      .build

    val records = Iterator.continually(reader.read())
      .takeWhile(_ != null)
      .toArray

    // save schema
    schemaIn = if(records.length > 0) {
      records.head.getSchema
    }
    else null

    records.map(_. toString)
  }

  def readTextFile(txtFilePath : String, schema: Schema) : Array[String] = {
    if(schema == null) {
      throw new IllegalArgumentException("Input Schema is missing to load the Json File")
    }
    val source = scala.io.Source.fromFile(txtFilePath)
    val lines = source.getLines().toArray
    source.close()
    lines
  }

  def writeTextFile(jsonLines: Array[String], jsonPath : String, schema: Schema):Unit = {
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
      FileUtility.moveFile(tmpFile.getAbsolutePath, jsonPath)
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFile.getAbsolutePath)
    }
  }

  def writeParquetFile(jsonLines: Array[String], parquetPath: String, schema: Schema): Unit = {
    if (jsonLines.isEmpty) return

    val tmpFile = File.createTempFile("tmp", "parquet")

    try {
      val writer: ParquetWriter[GenericRecord] = AvroParquetWriter
        .builder[GenericRecord](new Path(tmpFile.getAbsolutePath))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
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
      FileUtility.moveFile(tmpFile.getAbsolutePath, parquetPath)
    }
    catch {
      case ex : Exception => throw ex
    }
    finally {
      FileUtility.deleteFile(tmpFile.getAbsolutePath)
    }
  }
}

 */
