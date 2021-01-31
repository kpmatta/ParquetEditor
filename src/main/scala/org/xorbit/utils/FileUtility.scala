package org.xorbit.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.xorbit.utils.ResourceHandler.using

object FileUtility {
  def moveFile(srcPathName : String, dstPathName : String): Unit = {
    deleteFile(dstPathName)
    val srcFile = new File(srcPathName)
    if(srcFile.isDirectory) {
      FileUtils.moveDirectory(new File(srcPathName), new File(dstPathName))
    }
    else {
      FileUtils.moveFile(srcFile, new File(dstPathName))
    }
  }

  def deleteFile(filePathName : String): Boolean = {
    deleteFile(new File(filePathName))
  }

  def deleteFile(file : File): Boolean = {
    FileUtils.deleteQuietly(file)
  }

  def createTempFile(prefix: String, suffix: String): File = {
    File.createTempFile(prefix, suffix)
  }

  def createTempDirectory(prefix : String) = {
    Files.createTempDirectory(prefix)
  }

  def writeTextFile(lines: List[String], filePath : String) = {
    using(new BufferedWriter(new FileWriter(filePath))) { writer =>
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
      writer.flush()
    }
  }
}
