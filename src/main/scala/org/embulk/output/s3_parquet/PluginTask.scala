package org.embulk.output.s3_parquet

import java.util.{Optional, Map => JMap}

import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.config.{Config, ConfigDefault, Task}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi.time.TimestampFormatter.TimestampColumnOption

object PluginTask {

  trait ColumnOptionTask
      extends Task
      with TimestampColumnOption
      with LogicalTypeOption

  trait TypeOptionTask extends Task with LogicalTypeOption

  trait LogicalTypeOption {

    @Config("logical_type")
    def getLogicalType: Optional[String]
  }
}

trait PluginTask extends Task with TimestampFormatter.Task with Aws.Task {

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

  @Config("column_options")
  @ConfigDefault("{}")
  def getColumnOptions: JMap[String, PluginTask.ColumnOptionTask]

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

  @Config("type_options")
  @ConfigDefault("{}")
  def getTypeOptions: JMap[String, PluginTask.TypeOptionTask]
}
