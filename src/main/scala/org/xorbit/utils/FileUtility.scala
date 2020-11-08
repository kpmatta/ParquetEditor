package org.xorbit.utils

import java.io.File
import org.apache.commons.io.FileUtils

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
}
