package org.embulk.output.s3_parquet

import java.nio.file.{Files, Paths}
import java.util.{IllegalFormatException, Locale, List => JList}

import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.config.{
  ConfigDiff,
  ConfigException,
  ConfigSource,
  TaskReport,
  TaskSource
}
import org.embulk.output.s3_parquet.PluginTask.{
  ColumnOptionTask,
  TypeOptionTask
}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.parquet.{
  LogicalTypeHandlerStore,
  ParquetFileWriter
}
import org.embulk.spi.{
  Exec,
  OutputPlugin,
  PageReader,
  Schema,
  TransactionalPageOutput
}
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.util.Timestamps
import org.slf4j.{Logger, LoggerFactory}

import scala.util.chaining._

class S3ParquetOutputPlugin extends OutputPlugin {

  import implicits._

  val logger: Logger = LoggerFactory.getLogger(classOf[S3ParquetOutputPlugin])

  override def transaction(
      config: ConfigSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])

    configure(task, schema)
    control.run(task.dump)

    task.getCatalog.ifPresent { catalog =>
      val location =
        s"s3://${task.getBucket}/${task.getPathPrefix.replaceFirst("(.*/)[^/]+$", "$1")}"
      val parquetColumnLogicalTypes: Map[String, String] =
        Map.newBuilder[String, String].pipe { builder =>
          val cOptions: Map[String, ColumnOptionTask] = task.getColumnOptions
          val tOptions: Map[String, TypeOptionTask] = task.getTypeOptions
          schema.getColumns.foreach { c =>
            {
              for (o <- cOptions.get(c.getName);
                   logicalType <- o.getLogicalType)
                yield builder.addOne(c.getName -> logicalType)
            }.orElse {
              for (o <- tOptions.get(c.getType.getName);
                   logicalType <- o.getLogicalType)
                yield builder.addOne(c.getName -> logicalType)
            }
          }
          builder.result()
        }
      val cr = CatalogRegistrator(
        aws = Aws(task),
        task = catalog,
        schema = schema,
        location = location,
        compressionCodec = task.getCompressionCodec,
        parquetColumnLogicalTypes = parquetColumnLogicalTypes
      )
      cr.run()
    }

    Exec.newConfigDiff
  }

  private def configure(task: PluginTask, schema: Schema): Unit = {
    // sequence_format
    try String.format(task.getSequenceFormat, 0: Integer, 0: Integer)
    catch {
      case e: IllegalFormatException =>
        throw new ConfigException(
          s"Invalid sequence_format: ${task.getSequenceFormat}",
          e
        )
    }

    // compression_codec
    CompressionCodecName
      .values()
      .find(v =>
        v.name()
          .toLowerCase(Locale.ENGLISH)
          .equals(task.getCompressionCodecString)
      ) match {
      case Some(v) => task.setCompressionCodec(v)
      case None =>
        val unsupported: String = task.getCompressionCodecString
        val supported: String = CompressionCodecName
          .values()
          .map(v => s"'${v.name().toLowerCase}'")
          .mkString(", ")
        throw new ConfigException(
          s"'$unsupported' is unsupported: `compression_codec` must be one of [$supported]."
        )
    }

    // column_options
    task.getColumnOptions.forEach { (k: String, opt: ColumnOptionTask) =>
      val c = schema.lookupColumn(k)
      val useTimestampOption =
        opt.getFormat.isDefined || opt.getTimeZoneId.isDefined
      if (!c.getType.getName.equals("timestamp") && useTimestampOption) {
        throw new ConfigException(s"column:$k is not 'timestamp' type.")
      }
    }

    // canned_acl
    CannedAccessControlList
      .values()
      .find(v => v.toString.equals(task.getCannedAclString)) match {
      case Some(v) => task.setCannedAcl(v)
      case None =>
        val unsupported: String = task.getCannedAclString
        val supported: String = CannedAccessControlList
          .values()
          .map(v => s"'${v.toString}'")
          .mkString(", ")
        throw new ConfigException(
          s"'$unsupported' is unsupported: `canned_acl` must be one of [$supported]."
        )
    }
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = {
    throw new UnsupportedOperationException(
      "s3_parquet output plugin does not support resuming"
    )
  }

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    successTaskReports.foreach { tr =>
      logger.info(
        s"Created: s3://${tr.get(classOf[String], "bucket")}/${tr.get(classOf[String], "key")}, "
          + s"version_id: ${tr.get(classOf[String], "version_id", null)}, "
          + s"etag: ${tr.get(classOf[String], "etag", null)}"
      )
    }
  }

  override def open(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int
  ): TransactionalPageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])
    val bufferDir: String = task.getBufferDir.getOrElse(
      Files.createTempDirectory("embulk-output-s3_parquet-").toString
    )
    val bufferFile: String = Paths
      .get(bufferDir, s"embulk-output-s3_parquet-task-$taskIndex-0.parquet")
      .toString
    val destS3bucket: String = task.getBucket
    val destS3Key: String =
      s"${task.getPathPrefix}${task.getSequenceFormat.format(taskIndex, 0)}${task.getFileExt}"

    val pageReader: PageReader = new PageReader(schema)
    val aws: Aws = Aws(task)
    val timestampFormatters: Seq[TimestampFormatter] = Timestamps
      .newTimestampColumnFormatters(task, schema, task.getColumnOptions)
    val logicalTypeHandlers = LogicalTypeHandlerStore.fromEmbulkOptions(
      task.getTypeOptions,
      task.getColumnOptions
    )
    val parquetWriter: ParquetWriter[PageReader] =
      ContextClassLoaderSwapper.usingPluginClass {
        ParquetFileWriter
          .builder()
          .withPath(bufferFile)
          .withSchema(schema)
          .withLogicalTypeHandlers(logicalTypeHandlers)
          .withTimestampFormatters(timestampFormatters)
          .withCompressionCodec(task.getCompressionCodec)
          .withDictionaryEncoding(
            task.getEnableDictionaryEncoding.orElse(
              ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED
            )
          )
          .withDictionaryPageSize(
            task.getPageSize.orElse(
              ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE
            )
          )
          .withMaxPaddingSize(
            task.getMaxPaddingSize.orElse(
              ParquetWriter.MAX_PADDING_SIZE_DEFAULT
            )
          )
          .withPageSize(
            task.getPageSize.orElse(ParquetProperties.DEFAULT_PAGE_SIZE)
          )
          .withRowGroupSize(
            task.getBlockSize.orElse(ParquetWriter.DEFAULT_BLOCK_SIZE)
          )
          .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
          .withWriteMode(
            org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE
          )
          .withWriterVersion(ParquetProperties.DEFAULT_WRITER_VERSION)
          .build()
      }

    logger.info(
      s"Local Buffer File: $bufferFile, Destination: s3://$destS3bucket/$destS3Key"
    )

    S3ParquetPageOutput(
      bufferFile,
      pageReader,
      parquetWriter,
      aws,
      destS3bucket,
      destS3Key
    )
  }

}
