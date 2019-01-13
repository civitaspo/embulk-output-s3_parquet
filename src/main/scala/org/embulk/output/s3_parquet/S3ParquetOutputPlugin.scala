package org.embulk.output.s3_parquet


import java.util.{IllegalFormatException, Locale, List => JList, Map => JMap}

import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter.Builder
import org.embulk.config.{Config, ConfigDefault, ConfigDiff, ConfigException, ConfigSource, Task, TaskReport, TaskSource}
import org.embulk.output.s3_parquet.S3ParquetOutputPlugin.PluginTask
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.spi.{Exec, OutputPlugin, Page, PageBuilder, PageReader, Schema, TransactionalPageOutput}
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.time.TimestampFormatter.TimestampColumnOption

object S3ParquetOutputPlugin {

  trait PluginTask
    extends Task
    with TimestampFormatter.Task
    with Aws.Task {

    @Config("bucket")
    def getBucket: String

    @Config("path_prefix")
    def getPathPrefix: String

    @Config("sequence_format")
    @ConfigDefault("\"%03d.%02d.\"")
    def getSequenceFormat: String

    @Config("file_ext")
    @ConfigDefault("\"parquet\"")
    def getFileExt: String

    @Config("compression_codec")
    @ConfigDefault("\"uncompressed\"")
    def getCompressionCodecString: String

    def setCompressionCodec(v: CompressionCodec): Unit

    def getCompressionCodec: CompressionCodec

    @Config("column_options")
    @ConfigDefault("{}")
    def getColumnOptions: JMap[String, TimestampColumnOption]

    @Config("canned_acl")
    @ConfigDefault("\"private\"")
    def getCannedAclString: String

    def setCannedAcl(v: CannedAccessControlList): Unit

    def getCannedAcl: CannedAccessControlList

    @Config("block_size")
    @ConfigDefault("134217728") // 128MB
    def getBlockSize: Long

    @Config("page_size")
    @ConfigDefault("1048576") // 1MB
    def getPageSize: Long

  }

}

class S3ParquetOutputPlugin
  extends OutputPlugin {

  override def transaction(config: ConfigSource,
                           schema: Schema,
                           taskCount: Int,
                           control: OutputPlugin.Control): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])

    withPluginContextClassLoader {
      configure(task, schema)
      control.run(task.dump)
    }

    Exec.newConfigDiff
  }

  private def configure(task: PluginTask,
                        schema: Schema): Unit = {
    // sequence_format
    try String.format(Locale.ENGLISH, task.getSequenceFormat, 0, 0)
    catch {
      case e: IllegalFormatException => throw new ConfigException(s"Invalid sequence_format: ${task.getSequenceFormat}", e)
    }

    // compression_codec
    CompressionCodec.values().find(v => v.name().toLowerCase(Locale.ENGLISH).equals(task.getCompressionCodecString)) match {
      case Some(v) => task.setCompressionCodec(v)
      case None    =>
        val unsupported: String = task.getCompressionCodecString
        val supported: String = CompressionCodec.values().map(v => s"'${v.name().toLowerCase}'").mkString(", ")
        throw new ConfigException(s"'$unsupported' is unsupported: `compression_codec` must be one of [$supported].")
    }

    // column_options
    task.getColumnOptions.forEach { (k: String,
                                     _) =>
      val c = schema.lookupColumn(k)
      if (!c.getType.getName.equals("timestamp")) throw new ConfigException(s"column:$k is not 'timestamp' type.")
    }

    // canned_acl
    CannedAccessControlList.values().find(v => v.toString.equals(task.getCannedAclString)) match {
      case Some(v) => task.setCannedAcl(v)
      case None    =>
        val unsupported: String = task.getCannedAclString
        val supported: String = CannedAccessControlList.values().map(v => s"'${v.toString}'").mkString(", ")
        throw new ConfigException(s"'$unsupported' is unsupported: `canned_acl` must be one of [$supported].")
    }
  }

  override def resume(taskSource: TaskSource,
                      schema: Schema,
                      taskCount: Int,
                      control: OutputPlugin.Control): ConfigDiff = {
    throw new UnsupportedOperationException("s3_parquet output plugin does not support resuming")
  }

  override def cleanup(taskSource: TaskSource,
                       schema: Schema,
                       taskCount: Int,
                       successTaskReports: JList[TaskReport]): Unit = {}

  override def open(taskSource: TaskSource,
                    schema: Schema,
                    taskIndex: Int): TransactionalPageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])
    throw new UnsupportedOperationException("S3ParquetOutputPlugin.run method is not implemented yet")
  }
}
