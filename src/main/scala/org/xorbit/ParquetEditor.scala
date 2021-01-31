package org.xorbit

import java.awt.event.ActionEvent
import java.awt.{BorderLayout, Dimension, Font}
import java.io.File

import javax.swing._
import javax.swing.text.JTextComponent
import org.apache.spark.sql.types.StructType
import org.xorbit.ParquetEditor.saveFile
import org.xorbit.spark.ReadWriteParquet._
//import org.xorbit.parquet_avro.PAEditor._
import java.awt.Color

import scala.util.{Failure, Success, Try}

object ParquetEditor {
  private var m_frame: JFrame = _
  private var m_textArea: JTextArea = _
  private var m_textSchemaIn: JTextField = _
  private var m_textSchemaOut: JTextField = _
  private var inputSchema : Option[StructType] = None
  private var outputSchema : Option[StructType] = None
  private var filePathOpt: Option[String] = None
  private var fileTypeOpt: Option[String] = None

  private val PARQUET_TYPE = "parquet"
  private val JSON_TYPE = "json"

  def main(args: Array[String]): Unit = {
    m_frame = new JFrame("Parquet Editor")
    createMenu(m_frame)
    createLayOut(m_frame)
  }

  def setText(txtComp: JTextComponent, text: String): Unit = {
    txtComp.setText(text)
    txtComp.setCaretPosition(0)
  }

  def clearText(txtComp : JTextComponent) : Unit = {
    setText(txtComp, "")
  }

  def getText(txtComp : JTextComponent): String = {
    txtComp.getText()
  }

  def createLayOut(frame: JFrame): Unit = {
    m_textArea = new JTextArea()
    m_textArea.setText("")
    m_textArea.setFont(new Font("Sans Serif", Font.PLAIN, 16))

    val schemaPanel = new JPanel()
    schemaPanel.setLayout(new BoxLayout(schemaPanel, BoxLayout.Y_AXIS))
    val inPanel = new JPanel(new BorderLayout())
    m_textSchemaIn = new JTextField()
    m_textSchemaIn.setBorder(BorderFactory.createLineBorder(Color.GRAY, 1))
    m_textSchemaIn.setText("")
    m_textSchemaIn.setEnabled(false)
    val lblSchemaIn = new JLabel(" Input Schema Path: ")
    lblSchemaIn.setPreferredSize(new Dimension(150, lblSchemaIn.getHeight))
    inPanel.add(lblSchemaIn, BorderLayout.WEST)
    inPanel.add(m_textSchemaIn, BorderLayout.CENTER)
    schemaPanel.add(inPanel)

    val scrollPane = new JScrollPane(m_textArea)
    frame.getContentPane.add(schemaPanel, BorderLayout.NORTH)
    frame.getContentPane.add(scrollPane, BorderLayout.CENTER)
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.setSize(new Dimension(1400, 800))
    frame.setLocationRelativeTo(null)
    frame.setVisible(true)
  }

  def clearUI(): Unit = {
    clearText(m_textArea)
    setTitle(m_frame, "")
    clearText(m_textSchemaIn)
    clearText(m_textSchemaOut)
    filePathOpt = None
    fileTypeOpt = None
    inputSchema = None
    outputSchema = None
  }

  def onClose(frame: JFrame): Unit = {
    clearUI()
  }

  def onExit(): Unit = {
    System.exit(0)
  }

  def setTitle(frame: JFrame, title: String): Unit = {
    val titleStr = if(title.isEmpty) "Parquet Editor" else s"$title - Parquet Editor"
    frame.setTitle(titleStr)
  }

  def setPathInTitle(frame: JFrame, filePath: String): Unit = {
    val file = new File(filePath)
    setTitle(frame, file.getName + " [" + file.getPath + "]")
  }

  def showMessageDialog(message : String, ex: Throwable = null):Unit ={
    val msg = List(
      Option(message),
      Try(ex.getMessage).toOption,
      Try(ex.getCause.getMessage).toOption)
      .flatten
      .mkString(System.lineSeparator())
    showMessageDialog(msg)
  }

  def showMessageDialog(msg: String): Unit = {
    val msgLines = msg.split("\\R").length
    val scrollHeight = 20
    val lineHeight = 20
    val maxHeight = 200
    val paneHeight = math.min(msgLines * lineHeight, maxHeight) + scrollHeight
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
        if(inputSchema.isEmpty) {
          throw new IllegalArgumentException("Input Schema is missing to load the Json File")
        }
        readTextFile(file.getAbsolutePath)
      case PARQUET_TYPE =>
        val (lines, schema) = readParquetFile(file.getAbsolutePath)
        m_textSchemaIn.setText("Schema inferred from parquet file")
        inputSchema = Some(schema)
        lines
      case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
    }

    filePathOpt = Option(file.getAbsolutePath)
    fileTypeOpt = Option(fileType)
    setText(m_textArea, jsonLines.mkString(System.lineSeparator()))
    setPathInTitle(m_frame, file.getAbsolutePath)
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
    Try(openFile(frame, PARQUET_TYPE)).failed.foreach{ ex =>
      showMessageDialog("Error opening the Parquet file", ex)
    }
  }

  def onOpenJsonFile(frame: JFrame): Unit = {
    Try(openFile(frame, JSON_TYPE)).failed.foreach{ ex =>
      showMessageDialog("Error opening the Json file", ex)
    }
  }

  def onSave(): Unit = {
    Option (getText(m_textArea)).filter(_.trim.nonEmpty).foreach { data =>
      val retVal = JOptionPane.showConfirmDialog(m_frame, "Are you sure to overwrite the file?")
      if (retVal == 0) {
        val lines = data.split("\\R").toList
        Try(saveFile(lines, filePathOpt.get, fileTypeOpt.get)) match {
          case Success(_) =>
            showMessageDialog("File saved")
          case Failure(ex) =>
            showMessageDialog("Error Saving the file", ex)
        }
      }
      // reload the saved file again
      openFile(new File(filePathOpt.get), fileTypeOpt.get)
    }
  }

  def getSchemaOut: Option[StructType] = {
    Option(outputSchema.getOrElse(inputSchema.get))
  }

  def saveFile(lines: List[String], fileName: String, fileType: String): Unit = {
    if (fileName.isEmpty)
      return

    val schema = getSchemaOut
    if (getSchemaOut.isEmpty) {
      throw new IllegalArgumentException("Input or Output Schema is required to save the file")
    }

    fileType match {
      case PARQUET_TYPE => writeParquetFile(lines, fileName, schema.get)
      case JSON_TYPE => writeJsonFile(lines, fileName, schema.get)
      case _ => throw new IllegalArgumentException("Unknown file type : " + fileType)
    }
  }

  def saveAsFile(frame: JFrame, lines: List[String], fileType: String): Option[String] = {
    val fileChooser = new JFileChooser(getParentPath)
    fileChooser.showSaveDialog(frame) match {
      case JFileChooser.APPROVE_OPTION =>
        val fileToSave = fileType match {
          case JSON_TYPE => fileChooser.getSelectedFile.getAbsolutePath.stripSuffix(".json") + ".json"
          case _ => fileChooser.getSelectedFile.getAbsolutePath
        }

        saveFile(lines, fileToSave, fileType)
        filePathOpt = Option(fileToSave)
        fileTypeOpt = Option(fileType)
        setPathInTitle(m_frame, fileToSave)
        filePathOpt
      case _ => None
    }
  }

  def generateSchema(frame: JFrame, schema: StructType): Option[String] = {
    val fileChooser = new JFileChooser(getParentPath)
    fileChooser.showSaveDialog(frame) match {
      case JFileChooser.APPROVE_OPTION =>
        val schemaFile = fileChooser
          .getSelectedFile
          .getAbsolutePath.stripSuffix(".json") + ".json"
        writeSchema(schema, schemaFile)
        Some(schemaFile)
      case _ => None
    }
  }

  def onSaveAsJson(frame: JFrame): Unit = {
    Option (getText(m_textArea)).filter(_.trim.nonEmpty).foreach { data =>
      val lines = data.split("\\R").toList
      Try( saveAsFile(frame, lines, JSON_TYPE) )match {
        case Success(pathOpt) => pathOpt.foreach( path => showMessageDialog(s"[$path] File Saved !!!"))
        case Failure(ex) => showMessageDialog("Error saving Json file", ex)
      }

      openFile(new File(filePathOpt.get), fileTypeOpt.get)
    }
  }

  def onSaveAsParquet(frame: JFrame): Unit = {
    Option (getText(m_textArea)).filter(_.trim.nonEmpty).foreach { data =>
      val lines = data.split("\\R").toList
      Try(saveAsFile(frame, lines, PARQUET_TYPE)) match {
        case Success(pathOpt) => pathOpt.foreach( path => showMessageDialog(s"[$path] File Saved !!!"))
        case Failure(ex) => showMessageDialog("Error saving Parquet file", ex)
      }

      openFile(new File(filePathOpt.get), fileTypeOpt.get)
    }
  }

  def onGenerateSchema(frame: JFrame): Unit = {
    inputSchema match {
      case Some(schema) =>
        Try(generateSchema(frame, schema)) match {
          case Success(Some(schemaFileName)) => showMessageDialog(s"Schema file is generated : $schemaFileName")
          case Failure(ex) =>  showMessageDialog("Error generating the Schema", ex)
          case _ =>
        }
      case None =>
        showMessageDialog("No schema found: Load a parquet file to generate schema")
    }
  }

  def browseFile(frame: JFrame): Option[String] = {
    val fileChooser = new JFileChooser(getDefaultPath)
    fileChooser.showOpenDialog(frame) match {
      case JFileChooser.APPROVE_OPTION => Some(fileChooser.getSelectedFile.getAbsolutePath)
      case _ => None
    }
  }

  def onLoadSchema(frame: JFrame): (Option[StructType], Option[String]) = {
    browseFile(frame) match {
      case Some(path) =>
        Try(readSchema(path)) match {
          case Success(schema) => (Option(schema), Option(path))
          case Failure(ex) =>
            showMessageDialog("Error loading the schema", ex)
            (None, None)
        }
      case _ => (None, None)
    }
  }

  def onLoadInputSchema(frame: JFrame): Unit = {
    val (schema, pathOpt) = onLoadSchema(frame)
    inputSchema = schema
    pathOpt.foreach( setText(m_textSchemaIn, _) )
  }

  def onLoadOutputSchema(frame: JFrame): Unit = {
    val (schema, pathOpt) = onLoadSchema(frame)
    outputSchema = schema
    pathOpt.foreach( setText(m_textSchemaOut, _) )
  }

  def createMenu(frame: JFrame): Unit = {
    val menuBar = new JMenuBar()
    val menu = new JMenu("File")
    val miOpenParquet = new JMenuItem("Open Parquet")
    val miOpenJson = new JMenuItem("Open Json")
    val miSave = new JMenuItem("Save")
    val miSaveAsParquet = new JMenuItem("Save As Parquet")
    val miSaveAsJson = new JMenuItem("Save As Json")
    val miOpenSchema = new JMenuItem("Open Schema")
    val miSaveSchema = new JMenuItem("Generate Schema")
    val miClose = new JMenuItem("Close")
    val miExit = new JMenuItem("Exit")

    miExit.addActionListener((_: ActionEvent) => onExit())
    miClose.addActionListener((_: ActionEvent) => onClose(frame))
    miOpenParquet.addActionListener((_: ActionEvent) => onOpenParquetFile(frame))
    miOpenJson.addActionListener((_: ActionEvent) => onOpenJsonFile(frame))
    miSave.addActionListener((_: ActionEvent) => onSave())
    miSaveAsParquet.addActionListener((_: ActionEvent) => onSaveAsParquet(frame))
    miSaveAsJson.addActionListener((_: ActionEvent) => onSaveAsJson(frame))
    miOpenSchema.addActionListener((_: ActionEvent) => onLoadInputSchema(frame))
    miSaveSchema.addActionListener((_: ActionEvent) => onGenerateSchema(frame))

    menu.add(miOpenParquet)
    menu.add(miOpenJson)
    menu.add(miSave)
    menu.add(miSaveAsParquet)
    menu.add(miSaveAsJson)
    menu.add(miOpenSchema)
    menu.add(miSaveSchema)
    menu.add(miClose)
    menu.add(miExit)
    menuBar.add(menu)
    frame.setJMenuBar(menuBar)
  }
}
