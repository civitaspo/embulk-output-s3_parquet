package org.embulk.output.s3_parquet.parquet

import java.lang.{StringBuilder => JStringBuilder}
import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  ConfigSource,
  Task => EmbulkTask
}
import org.embulk.output.s3_parquet.implicits
import org.embulk.output.s3_parquet.parquet.ParquetFileWriteSupport.WriterBuilder
import org.embulk.spi.{Column, ColumnVisitor, PageReader, Schema}
import org.embulk.spi.`type`.{TimestampType, Type, Types}
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.util.Timestamps
import org.slf4j.Logger

object ParquetFileWriteSupport {

  import implicits._

  trait Task extends TimestampFormatter.Task with EmbulkTask {
    @Config("column_options")
    @ConfigDefault("{}")
    def getRawColumnOptions: JMap[String, ConfigSource]

    def getColumnOptions: JMap[String, ParquetColumnType.Task]
    def setColumnOptions(
        columnOptions: JMap[String, ParquetColumnType.Task]
    ): Unit

    @Config("type_options")
    @ConfigDefault("{}")
    def getRawTypeOptions: JMap[String, ConfigSource]

    def getTypeOptions: JMap[String, ParquetColumnType.Task]
    def setTypeOptions(typeOptions: JMap[String, ParquetColumnType.Task]): Unit
  }

  case class WriterBuilder(path: Path, writeSupport: ParquetFileWriteSupport)
      extends ParquetWriter.Builder[PageReader, WriterBuilder](path) {
    override def self(): WriterBuilder = this
    override def getWriteSupport(
        conf: Configuration
    ): WriteSupport[PageReader] = writeSupport
  }

  def configure(task: Task): Unit = {
    task.setColumnOptions(task.getRawColumnOptions.map {
      case (columnName, config) =>
        columnName -> ParquetColumnType.loadConfig(config)
    })
    task.setTypeOptions(task.getRawTypeOptions.map {
      case (columnType, config) =>
        columnType -> ParquetColumnType.loadConfig(config)
    })
  }

  private def validateTask(task: Task, schema: Schema): Unit = {
    if (task.getColumnOptions == null || task.getTypeOptions == null)
      assert(false)

    task.getTypeOptions.keys.foreach(
      embulkType
    ) // throw ConfigException if unknown type name is found.

    task.getColumnOptions.foreach {
      case (c: String, t: ParquetColumnType.Task) =>
        val column: Column = schema.lookupColumn(c) // throw ConfigException if columnName does not exist.

        if (t.getFormat.isDefined || t.getTimeZoneId.isDefined) {
          if (!column.getType.isInstanceOf[TimestampType]) {
            // NOTE: Warning is better instead of throwing.
            throw new ConfigException(
              s"The type of column{name:${column.getName},type:${column.getType.getName}} is not 'timestamp'," +
                " but timestamp options (\"format\" or \"timezone\") are set."
            )
          }
        }
    }
  }

  private def embulkType(typeName: String): Type = {
    Seq(
      Types.BOOLEAN,
      Types.STRING,
      Types.LONG,
      Types.DOUBLE,
      Types.TIMESTAMP,
      Types.JSON
    ).foreach { embulkType =>
      if (embulkType.getName.equals(typeName)) return embulkType
    }
    throw new ConfigException(s"Unknown embulk type: $typeName.")
  }

  def apply(task: Task, schema: Schema): ParquetFileWriteSupport = {
    validateTask(task, schema)

    val parquetSchema: Map[Column, ParquetColumnType] = schema.getColumns.map {
      c: Column =>
        c -> task.getColumnOptions.toMap
          .get(c.getName)
          .orElse(task.getTypeOptions.toMap.get(c.getType.getName))
          .flatMap(ParquetColumnType.fromTask)
          .getOrElse(DefaultColumnType)
    }.toMap
    val timestampFormatters: Seq[TimestampFormatter] = Timestamps
      .newTimestampColumnFormatters(task, schema, task.getColumnOptions)
    new ParquetFileWriteSupport(schema, parquetSchema, timestampFormatters)
  }
}

case class ParquetFileWriteSupport private (
    schema: Schema,
    parquetSchema: Map[Column, ParquetColumnType],
    timestampFormatters: Seq[TimestampFormatter]
) extends WriteSupport[PageReader] {

  import implicits._

  private val messageType: MessageType =
    new MessageType("embulk", schema.getColumns.map { c =>
      parquetSchema(c).primitiveType(c)
    })

  private var current: RecordConsumer = _

  def showOutputSchema(logger: Logger): Unit = {
    val sb = new JStringBuilder()
    sb.append("=== Output Parquet Schema ===\n")
    messageType.writeToStringBuilder(sb, null) // NOTE: indent is not used.
    sb.append("=============================\n")
    sb.toString.split("\n").foreach(logger.info)
  }

  override def init(configuration: Configuration): WriteContext = {
    val metadata: Map[String, String] = Map.empty // NOTE: When is this used?
    new WriteContext(messageType, metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
    current = recordConsumer

  override def write(record: PageReader): Unit = {
    writingRecord {
      schema.visitColumns(new ColumnVisitor {
        override def booleanColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column)
            .consumeBoolean(current, record.getBoolean(column))
        }
        override def longColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column).consumeLong(current, record.getLong(column))
        }
        override def doubleColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column).consumeDouble(current, record.getDouble(column))
        }
        override def stringColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column).consumeString(current, record.getString(column))
        }
        override def timestampColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column).consumeTimestamp(
            current,
            record.getTimestamp(column),
            timestampFormatters(column.getIndex)
          )
        }
        override def jsonColumn(column: Column): Unit = nullOr(column) {
          parquetSchema(column).consumeJson(current, record.getJson(column))
        }
        private def nullOr(column: Column)(f: => Unit): Unit =
          if (!record.isNull(column)) writingColumn(column)(f)
      })
    }
  }

  private def writingRecord(f: => Unit): Unit = {
    current.startMessage()
    f
    current.endMessage()
  }

  private def writingColumn(column: Column)(f: => Unit): Unit = {
    current.startField(column.getName, column.getIndex)
    f
    current.endField(column.getName, column.getIndex)
  }

  def newWriterBuilder(pathString: String): WriterBuilder =
    WriterBuilder(new Path(pathString), this)
}
