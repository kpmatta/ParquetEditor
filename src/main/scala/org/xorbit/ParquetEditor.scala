package org.xorbit

import java.awt.event.{ActionEvent, ActionListener}
import java.awt.{BorderLayout, Dimension, Font}
import java.io.{BufferedWriter, File, FileWriter}

import javax.swing._
import org.apache.spark.sql.types.StructType
import org.xorbit.spark.ReadWriteParquet._
//import org.xorbit.parquet_avro.PAEditor._
import java.awt.Color

import scala.util.{Failure, Success, Try}

object ParquetEditor {
  private var m_menuBar: JMenuBar = _
  private var m_menu: JMenu = _
  private var m_frame: JFrame = _
  private var m_textArea: JTextArea = _
  private var m_textSchemaIn: JTextField = _
  private var m_textSchemaOut: JTextField = _
  private var m_btnSchemaIn: JButton = _
  private var m_btnSchemaOut: JButton = _

  private var filePathOpt: Option[String] = None
  private var fileTypeOpt: Option[String] = None
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
    m_textArea.setFont(new Font("Sans Serif", Font.PLAIN, 16))
    val schemaPanel = new JPanel()
    schemaPanel.setLayout(new BoxLayout(schemaPanel, BoxLayout.Y_AXIS))
    val inPanel = new JPanel(new BorderLayout())
    val outPanel = new JPanel(new BorderLayout())
    m_textSchemaIn = new JTextField()
    m_textSchemaIn.setBorder(BorderFactory.createLineBorder(Color.GRAY, 1))
    m_textSchemaIn.setText("")
    m_textSchemaIn.setEnabled(false)
    val lblSchemaIn = new JLabel(" Input Schema Path: ")
    lblSchemaIn.setPreferredSize(new Dimension(150, lblSchemaIn.getHeight))
    m_btnSchemaIn = new JButton("Browse")
    inPanel.add(lblSchemaIn, BorderLayout.WEST)
    inPanel.add(m_textSchemaIn, BorderLayout.CENTER)
    inPanel.add(m_btnSchemaIn, BorderLayout.EAST)

    m_textSchemaOut = new JTextField()
    m_textSchemaOut.setBorder(BorderFactory.createLineBorder(Color.GRAY, 1))
    m_textSchemaOut.setText("")
    m_textSchemaOut.setEnabled(false)
    val lblSchemaOut = new JLabel(" Output Schema Path: ")
    lblSchemaOut.setPreferredSize(new Dimension(150, lblSchemaOut.getHeight))
    m_btnSchemaOut = new JButton("Browse")
    outPanel.add(lblSchemaOut, BorderLayout.WEST)
    outPanel.add(m_textSchemaOut, BorderLayout.CENTER)
    outPanel.add(m_btnSchemaOut, BorderLayout.EAST)
    schemaPanel.add(inPanel)
    schemaPanel.add(outPanel)
    val scrollPane = new JScrollPane(m_textArea)

    m_btnSchemaOut.addActionListener((e: ActionEvent) => onLoadOutputSchema(frame))
    m_btnSchemaIn.addActionListener((e: ActionEvent) => onLoadInputSchema(frame))

    frame.getContentPane.add(schemaPanel, BorderLayout.NORTH)
    frame.getContentPane.add(scrollPane, BorderLayout.CENTER)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.setSize(new Dimension(1000, 800))
    frame.setLocationRelativeTo(null)
    frame.setVisible(true)
  }

  def clearUI(): Unit = {
    setText("")
    setTitle("")
    m_textArea.setText("")
    m_textSchemaIn.setText("")
    m_textSchemaOut.setText("")
    filePathOpt = None
    fileTypeOpt = None
  }

  def onClose(frame: JFrame): Unit = {
    clearUI()
    cleanUp()
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

  def showMessageDialog(message : String, ex: Throwable):Unit ={
    val msg =
      s"""
         |$message
         |${ex.getMessage}
         |${ex.getCause.toString}
      """.stripMargin

    showMessageDialog(msg)
  }

  def showMessageDialog(ex: Throwable): Unit = {
    val msg =
      s"""
        |${ex.getMessage}
        |${ex.getCause.toString}
      """.stripMargin

    showMessageDialog(msg)
  }

  def showMessageDialog(msg: String): Unit = {
    val msgLines = msg.split("\\R").length
    val paneHeight = if (msgLines < 10) msgLines * 20 else 200
    val txtArea = new JTextArea(msg)
    val scrollPane = new JScrollPane(txtArea)
    scrollPane.setPreferredSize(new Dimension(620, paneHeight))
    JOptionPane.showMessageDialog(m_frame, scrollPane)
  }

  def getParentPath: String = {
    filePathOpt match {
      case Some(path) => new File(path).getParent
      case None => System.getProperty("user.dir")
    }
  }

  def getDefaultPath: String = {
    filePathOpt match {
      case Some(path) => path
      case None => System.getProperty("user.dir")
    }
  }

  def openFile(file: File, fileType: String): Unit = {
    val jsonLines = fileType match {
      case JSON_TYPE =>
        val schema = getSchemaIn
        readTextFile(file.getAbsolutePath, schema)
      case PARQUET_TYPE =>
        val lines = readParquetFile(file.getAbsolutePath)
        m_textSchemaIn.setText("Schema inferred from parquet file")
        lines
      case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
    }

    filePathOpt = Option(file.getAbsolutePath)
    fileTypeOpt = Option(fileType)
    setText(jsonLines.mkString(System.lineSeparator()))
    setTitle(file)
  }

  def openFile(frame: JFrame, fileType: String): Unit = {
    val fileChooser = new JFileChooser(getDefaultPath)
    if(fileType.equalsIgnoreCase(PARQUET_TYPE)) {
      fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY)
      fileChooser.setAcceptAllFileFilterUsed(false)
    }

    val option = fileChooser.showOpenDialog(frame)
    if (option == JFileChooser.APPROVE_OPTION) {
      val file = fileChooser.getSelectedFile
      openFile(file, fileType)
    }
  }

  def onOpenParquetFile(frame: JFrame): Unit = {
    Try(openFile(frame, PARQUET_TYPE)) match {
      case Success(_) =>
      case Failure(ex) => showMessageDialog(ex)
    }
  }

  def onOpenJsonFile(frame: JFrame): Unit = {
    Try(openFile(frame, JSON_TYPE)) match {
      case Success(_) =>
      case Failure(ex) => showMessageDialog(ex)
    }
  }

  def onSave(): Unit = {
    if (getText.trim.isEmpty)
      return

    JOptionPane.showConfirmDialog(m_frame, "Are you sure to overwrite the file?") match {
      case 0 =>
        Try(saveFile(filePathOpt.get, fileTypeOpt.get)) match {
          case Success(_) =>
            showMessageDialog("File saved")
          case Failure(ex) =>
            showMessageDialog("Error Saving the file", ex)
        }
      case _ =>
    }
    // reload the saved file again
    openFile(new File(filePathOpt.get), fileTypeOpt.get)
  }

  def saveFile(fileName: String, fileType: String): Unit = {
    if (fileName.nonEmpty) {
      val schema = getSchemaOut
      if (schema.isEmpty) {
        throw new IllegalArgumentException("Schema file is missing to save the file")
      }

      val lines = getText.split("\\R")
      fileType match {
        case PARQUET_TYPE => writeParquetFile(lines, fileName, schema.get)
        case JSON_TYPE => writeTextFile(lines, fileName, schema.get)
        case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
      }
    }
  }

  def saveAsFile(frame: JFrame, fileType: String): Option[String] = {
    val fileChooser = new JFileChooser(getParentPath)
    val option = fileChooser.showSaveDialog(frame)

    if (option == JFileChooser.APPROVE_OPTION) {
      val fileToSave = if(fileType.equals(JSON_TYPE)) {
        if (fileChooser.getSelectedFile.getName.endsWith(fileType)) {
          fileChooser.getSelectedFile
        }
        else {
          new File(fileChooser.getSelectedFile.getAbsolutePath + "." + fileType)
        }
      }
      else {
        fileChooser.getSelectedFile
      }

      saveFile(fileToSave.getAbsolutePath, fileType)
      filePathOpt = Option(fileToSave.getAbsolutePath)
      fileTypeOpt = Option(fileType)
      setTitle(fileToSave)
      filePathOpt
    }
    else {
      None
    }
  }

  def generateSchema(frame: JFrame, schema: StructType): Option[String] = {
    val fileChooser = new JFileChooser(getParentPath)
    val option = fileChooser.showSaveDialog(frame)

    if (option == JFileChooser.APPROVE_OPTION) {
      val schemaFile = fileChooser.getSelectedFile.getAbsolutePath + ".json"
      writeSchema(schema, schemaFile)
      Some(schemaFile)
    }
    else
      None
  }

  def onSaveAsJson(frame: JFrame): Unit = {
    if (getText.trim.isEmpty)
      return

    try {
      saveAsFile(frame, JSON_TYPE).foreach { path =>
          showMessageDialog(s"[$path] File Saved !!!")
      }
    }
    catch {
      case ex : Exception => showMessageDialog(ex)
    }
    finally {
      openFile(new File(filePathOpt.get), fileTypeOpt.get)
    }
  }

  def onSaveAsParquet(frame: JFrame): Unit = {
    if (getText.trim.isEmpty)
      return

    try {
      saveAsFile(frame, PARQUET_TYPE).foreach { path =>
          showMessageDialog(s"[$path] File Saved !!!")
      }
    }
    catch {
      case ex : Exception => showMessageDialog(ex)
    }
    finally {
      openFile(new File(filePathOpt.get), fileTypeOpt.get)
    }
  }

  def onGenerateSchema(frame: JFrame): Unit = {
    try {
      getSchemaIn match {
        case Some(schema) =>
          generateSchema(frame, schema).foreach { schemaFileName =>
            showMessageDialog(s"Schema file is generated : $schemaFileName")
          }
        case None =>
          showMessageDialog("No schema found: Load a parquet file to generate schema")
      }
    }
    catch {
      case ex:Exception => showMessageDialog(ex)
    }
  }

  def browseFile(frame: JFrame): Option[String] = {
    val fileChooser = new JFileChooser(getDefaultPath)
    val option = fileChooser.showOpenDialog(frame)
    if (option == JFileChooser.APPROVE_OPTION) {
      Some(fileChooser.getSelectedFile.getAbsolutePath)
    }
    else None
  }

  def onLoadInputSchema(frame: JFrame): Unit = {
    Try {
      browseFile(frame) match {
        case Some(path) =>
          readInputSchema(path)
          m_textSchemaIn.setText(path)
        case None => showMessageDialog("File is empty")
      }
    } match {
      case Success(_) =>
      case Failure(ex) => showMessageDialog(ex)
    }
  }

  def onLoadOutputSchema(frame: JFrame): Unit = {
    Try {
      browseFile(frame) match {
        case Some(path) =>
          readOutputSchema(path)
          m_textSchemaOut.setText(path)
        case None => showMessageDialog("File is empty")
      }
    } match {
      case Success(_) =>
      case Failure(ex) => showMessageDialog(ex)
    }
  }

  def createMenu(frame: JFrame): Unit = {
    m_menuBar = new JMenuBar()
    m_menu = new JMenu("File")
    val miOpenParquet = new JMenuItem("Open Parquet")
    val miOpenJson = new JMenuItem("Open Json")
    val miSave = new JMenuItem("Save")
    val miSaveAsParquet = new JMenuItem("Save As Parquet")
    val miSaveAsJson = new JMenuItem("Save As Json")
    val miSaveSchema = new JMenuItem("Generate Schema")
    val miClose = new JMenuItem("Close")
    val miExit = new JMenuItem("Exit")

    miExit.addActionListener((e: ActionEvent) => onExit())
    miClose.addActionListener((e: ActionEvent) => onClose(frame))
    miOpenParquet.addActionListener((e: ActionEvent) => onOpenParquetFile(frame))
    miOpenJson.addActionListener((e: ActionEvent) => onOpenJsonFile(frame))
    miSave.addActionListener((e: ActionEvent) => onSave())
    miSaveAsParquet.addActionListener((e: ActionEvent) => onSaveAsParquet(frame))
    miSaveAsJson.addActionListener((e: ActionEvent) => onSaveAsJson(frame))
    miSaveSchema.addActionListener((e: ActionEvent) => onGenerateSchema(frame))

    m_menu.add(miOpenParquet)
    m_menu.add(miOpenJson)
    m_menu.add(miSave)
    m_menu.add(miSaveAsParquet)
    m_menu.add(miSaveAsJson)
    m_menu.add(miSaveSchema)
    m_menu.add(miClose)
    m_menu.add(miExit)
    m_menuBar.add(m_menu)
    frame.setJMenuBar(m_menuBar)
  }
}
