package org.xorbit.utils

import org.apache.commons.io.FileUtils
import java.io.File

object FileUtil {
  def moveFile(srcPathName : String, dstPathName : String): Unit = {
    moveFile(new File(srcPathName), new File(dstPathName))
  }

  def moveFile(srcFile : File, dstFile : File): Unit = {
    FileUtils.deleteQuietly(dstFile)
    FileUtils.moveFile(srcFile, dstFile)
  }

  def deleteFile(filePathName : String): Boolean = {
    deleteFile(new File(filePathName))
  }

  def deleteFile(file : File): Boolean = {
    FileUtils.deleteQuietly(file)
  }
}
