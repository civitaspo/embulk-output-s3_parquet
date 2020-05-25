package org.embulk.output.s3_parquet

import java.nio.file.{Files, Paths}
import java.util.{List => JList}

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.embulk.config.{ConfigDiff, ConfigSource, TaskReport, TaskSource}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.catalog.CatalogRegistrator
import org.embulk.output.s3_parquet.parquet.ParquetFileWriteSupport
import org.embulk.spi.{
  Exec,
  OutputPlugin,
  PageReader,
  Schema,
  TransactionalPageOutput
}
import org.slf4j.{Logger, LoggerFactory}

class S3ParquetOutputPlugin extends OutputPlugin {

  import implicits._

  val logger: Logger = LoggerFactory.getLogger(classOf[S3ParquetOutputPlugin])

  override def transaction(
      config: ConfigSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.loadConfig(config)
    val support: ParquetFileWriteSupport = ParquetFileWriteSupport(task, schema)
    support.showOutputSchema(logger)
    control.run(task.dump)

    task.getCatalog.ifPresent { catalog =>
      val location =
        s"s3://${task.getBucket}/${task.getPathPrefix.replaceFirst("(.*/)[^/]+$", "$1")}"
      val cr = CatalogRegistrator.fromTask(
        task = catalog,
        aws = Aws(task),
        schema = schema,
        location = location,
        compressionCodec = task.getCompressionCodec,
        defaultGlueTypes =
          support.parquetSchema.transform((k, v) => v.glueDataType(k))
      )
      cr.run()
    }

    Exec.newConfigDiff
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
    val task = PluginTask.loadTask(taskSource)
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
    val parquetWriter: ParquetWriter[PageReader] =
      ContextClassLoaderSwapper.usingPluginClass {
        ParquetFileWriteSupport(task, schema)
          .newWriterBuilder(bufferFile)
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
