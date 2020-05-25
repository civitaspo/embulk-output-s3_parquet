package org.embulk.output.s3_parquet

import java.util.{Locale, MissingFormatArgumentException, Optional}

import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  ConfigSource,
  Task,
  TaskSource
}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.catalog.CatalogRegistrator
import org.embulk.output.s3_parquet.parquet.ParquetFileWriteSupport

trait PluginTask extends Task with ParquetFileWriteSupport.Task with Aws.Task {

  @Config("bucket")
  def getBucket: String

  @Config("path_prefix")
  @ConfigDefault("\"\"")
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

  def getCompressionCodec: CompressionCodecName
  def setCompressionCodec(v: CompressionCodecName): Unit

  @Config("canned_acl")
  @ConfigDefault("\"private\"")
  def getCannedAclString: String

  def getCannedAcl: CannedAccessControlList
  def setCannedAcl(v: CannedAccessControlList): Unit

  @Config("block_size")
  @ConfigDefault("null")
  def getBlockSize: Optional[Int]

  @Config("page_size")
  @ConfigDefault("null")
  def getPageSize: Optional[Int]

  @Config("max_padding_size")
  @ConfigDefault("null")
  def getMaxPaddingSize: Optional[Int]

  @Config("enable_dictionary_encoding")
  @ConfigDefault("null")
  def getEnableDictionaryEncoding: Optional[Boolean]

  @Config("buffer_dir")
  @ConfigDefault("null")
  def getBufferDir: Optional[String]

  @Config("catalog")
  @ConfigDefault("null")
  def getCatalog: Optional[CatalogRegistrator.Task]
}

object PluginTask {

  def loadConfig(config: ConfigSource): PluginTask = {
    val task = config.loadConfig(classOf[PluginTask])
    // sequence_format
    try task.getSequenceFormat.format(0, 0)
    catch {
      case e: MissingFormatArgumentException =>
        throw new ConfigException(
          s"Invalid sequence_format: ${task.getSequenceFormat}",
          e
        )
    }

    // compression_codec
    CompressionCodecName
      .values()
      .find(
        _.name()
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

    // canned_acl
    CannedAccessControlList
      .values()
      .find(_.toString.equals(task.getCannedAclString)) match {
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

    ParquetFileWriteSupport.configure(task)
    task
  }

  def loadTask(taskSource: TaskSource): PluginTask =
    taskSource.loadTask(classOf[PluginTask])

}
