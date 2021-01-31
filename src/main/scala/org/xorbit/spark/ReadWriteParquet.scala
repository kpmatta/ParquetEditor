package org.xorbit.spark

import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary
import org.apache.spark.sql.SaveMode
import org.xorbit.utils.ResourceHandler._
import org.apache.spark.sql.types.{DataType, StructType}
import org.xorbit.utils.FileUtility

object ReadWriteParquet extends SparkObj {

  import spark.implicits._

  def readSchema(schemaFilePath: String): StructType = {
    using(scala.io.Source.fromFile(schemaFilePath)) { bs =>
      DataType.fromJson(bs.mkString).asInstanceOf[StructType]
    }
  }

  def writeSchema(schema: StructType, filePathName: String): Unit = {
    val schemaLines = schema.prettyJson.split("\\R").toList
    FileUtility.writeTextFile(schemaLines, filePathName)
  }

  def readParquetFile(parquetPath: String): (List[String], StructType) = {
    val df = spark.read.parquet(parquetPath)
    (df.toJSON.collect().toList,
      df.schema)
  }

  def readTextFile(txtFilePath: String): List[String] = {
    using(scala.io.Source.fromFile(txtFilePath)) {
      f => f.getLines().toList
    }
  }

  def writeJsonFile(jsonLines: List[String],
                    jsonPath: String,
                    schema: StructType): Unit = {
    Option(jsonLines).filter(_.nonEmpty).foreach { lines =>
      val tmpFile = FileUtility.createTempFile("tmp", ".json")

      try {
        val jsonDS = spark.createDataset(lines)
        val updatedJsonLines = spark.read
          .schema(schema)
          .option("mode", "FAILFAST")
          .json(jsonDS).toJSON
          .collect().toList

        FileUtility.writeTextFile(updatedJsonLines, tmpFile.getAbsolutePath)
        FileUtility.moveFile(tmpFile.getAbsolutePath, jsonPath)
      }
      finally {
        FileUtility.deleteFile(tmpFile.getAbsolutePath)
      }
    }
  }

  def writeParquetFile(jsonLines: List[String],
                       parquetPath: String,
                       schema: StructType): Unit = {
    Option(jsonLines).filter(_.nonEmpty).foreach { lines =>
      val jsonDS = spark.createDataset(lines)
      val tmpFolder = FileUtility.createTempDirectory("tmp")
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
      finally {
        FileUtility.deleteFile(tmpFilePath)
      }
    }
  }
}
