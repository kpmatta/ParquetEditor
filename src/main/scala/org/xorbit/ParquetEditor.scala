package org.xorbit

import java.awt.event.{ActionEvent, ActionListener}
import java.awt.{BorderLayout, Dimension}
import java.io.File

import javax.swing._
import org.xorbit.parquet.ReadWriteParquet.{readParquetFile, readTextFile, writeParquetFile, writeTextFile}

import scala.util.{Failure, Success, Try}

object ParquetEditor {
  private var m_menuBar: JMenuBar = _
  private var m_menu: JMenu = _
  private var m_frame: JFrame = _
  private var m_textArea: JTextArea = _
  private var m_filePath: String = _
  private var m_fileType: String = _
  private val PARQUET_TYPE = "parquet"
  private val JSON_TYPE = "json"

  def main(args: Array[String]): Unit = {
    m_frame = new JFrame("Parquet File Editor")

    createMenu(m_frame)
    createLayOut(m_frame)
  }

  def setText(text: String): Unit = {
    m_textArea.setText(text)
  }

  def getText: String = {
    m_textArea.getText()
  }

  def createLayOut(frame: JFrame): Unit = {
    m_textArea = new JTextArea()
    m_textArea.setText("")
    val scrollPane = new JScrollPane(m_textArea)
    frame.getContentPane.add(scrollPane, BorderLayout.CENTER)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.setSize(new Dimension(1000, 800))
    frame.setLocationRelativeTo(null)
    frame.setVisible(true)
  }

  def onClose(frame: JFrame): Unit = {
    setText("")
    setTitle("")
  }

  def onExit(): Unit = {
    System.exit(0)
  }

  def setTitle(title: String): Unit = {
    m_frame.setTitle(title)
  }

  def setTitle(file: File): Unit = {
    setTitle(file.getName + " [" + file.getPath + "]")
  }

  def showMessageDialog(msg: String): Unit = {
    JOptionPane.showMessageDialog(m_frame, msg)
  }

  def getDefaultPath: String = {
    if (m_filePath != null && m_filePath.nonEmpty) m_filePath.reverse.dropWhile(_ != '/').reverse
    else System.getProperty("user.dir")
  }

  def openFile(file: File, fileType: String): Unit = {
    val jsonLines = fileType match {
      case JSON_TYPE => readTextFile(file.getAbsolutePath)
      case PARQUET_TYPE => readParquetFile(file.getAbsolutePath)
      case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
    }

    m_filePath = file.getAbsolutePath
    m_fileType = fileType
    setText(jsonLines.mkString(System.lineSeparator()))
    setTitle(file)
  }

  def openFile(frame: JFrame, fileType: String): Unit = {
    val fileChooser = new JFileChooser(getDefaultPath)
    val option = fileChooser.showOpenDialog(frame)
    if (option == JFileChooser.APPROVE_OPTION) {
      val file = fileChooser.getSelectedFile
      openFile(file, fileType)
    }
  }

  def onOpenParquetFile(frame: JFrame): Unit = {
    openFile(frame, PARQUET_TYPE)
  }

  def onOpenJsonFile(frame: JFrame): Unit = {
    openFile(frame, JSON_TYPE)
  }

  def onSave(): Unit = {
    Try {
      saveFile(m_filePath, m_fileType)
      // reload the saved file
      openFile(new File(m_filePath), m_fileType)
    } match {
      case Success(_) => showMessageDialog("File saved")
      case Failure(exception) => showMessageDialog(s"Error Saving the file : \n ${exception.getMessage}")
    }
  }

  def saveFile(fileName: String, fileType: String): Unit = {
    if (fileName.nonEmpty) {
      val lines = getText.split(System.lineSeparator())
      fileType match {
        case PARQUET_TYPE => writeParquetFile(lines, fileName)
        case JSON_TYPE => writeTextFile(lines, fileName)
      }
    }
  }

  def saveAsFile(frame: JFrame, fileType: String): Unit = {
    val fileChooser = new JFileChooser(getDefaultPath)
    val option = fileChooser.showSaveDialog(frame)

    if (option == JFileChooser.APPROVE_OPTION) {
      val fileToSave = if (fileChooser.getSelectedFile.getName.endsWith(fileType)) {
        fileChooser.getSelectedFile
      }
      else {
        new File(fileChooser.getSelectedFile.getAbsolutePath + "." + fileType)
      }

      if (fileToSave.getAbsolutePath.nonEmpty) {
        val lines = getText.split(System.lineSeparator())
        fileType match {
          case JSON_TYPE => writeTextFile(lines, fileToSave.getAbsolutePath)
          case PARQUET_TYPE => writeParquetFile(lines, fileToSave.getAbsolutePath)
          case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
        }

        m_filePath = fileToSave.getAbsolutePath
        m_fileType = fileType
        setTitle(fileToSave)
      }
    }
  }

  def saveAsFileWithMessage(frame: JFrame, fileType: String): Unit = {
    Try {
      saveAsFile(frame, fileType)
      // reload the saved file
      openFile(new File(m_filePath), m_fileType)
    } match {
      case Success(_) => showMessageDialog("File saved")
      case Failure(exception) => showMessageDialog(s"Error Saving the file : \n ${exception.getMessage}")
    }
  }

  def onSaveAsJson(frame: JFrame): Unit = {
    saveAsFileWithMessage(frame, JSON_TYPE)
  }

  def onSaveAsParquet(frame: JFrame): Unit = {
    saveAsFileWithMessage(frame, PARQUET_TYPE)
  }

  def createMenu(frame: JFrame): Unit = {
    m_menuBar = new JMenuBar()
    m_menu = new JMenu("File")
    val miOpenParquet = new JMenuItem("Open Parquet")
    val miOpenJson = new JMenuItem("Open Json")
    val miSave = new JMenuItem("Save")
    val miSaveAsParquet = new JMenuItem("Save As Parquet")
    val miSaveAsJson = new JMenuItem("Save As Json")
    val miClose = new JMenuItem("Close")
    val miExit = new JMenuItem("Exit")


    miExit.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onExit()
    })

    miClose.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onClose(frame)
    })

    miOpenParquet.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onOpenParquetFile(frame)
    })

    miOpenJson.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onOpenJsonFile(frame)
    })

    miSave.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onSave()
    })

    miSaveAsParquet.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onSaveAsParquet(frame)
    })

    miSaveAsJson.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = onSaveAsJson(frame)
    })

    m_menu.add(miOpenParquet)
    m_menu.add(miOpenJson)
    m_menu.add(miSave)
    m_menu.add(miSaveAsParquet)
    m_menu.add(miSaveAsJson)
    m_menu.add(miClose)
    m_menu.add(miExit)
    m_menuBar.add(m_menu)
    frame.setJMenuBar(m_menuBar)
  }
}
