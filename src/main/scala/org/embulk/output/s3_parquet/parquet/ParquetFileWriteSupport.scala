package org.embulk.output.s3_parquet.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.embulk.spi.{PageReader, Schema}
import org.embulk.spi.time.TimestampFormatter

private[parquet] case class ParquetFileWriteSupport(
    schema: Schema,
    timestampFormatters: Seq[TimestampFormatter],
    logicalTypeHandlers: LogicalTypeHandlerStore = LogicalTypeHandlerStore.empty
) extends WriteSupport[PageReader] {

  import org.embulk.output.s3_parquet.implicits._

  private var currentParquetFileWriter: ParquetFileWriter = _

  override def init(configuration: Configuration): WriteContext = {
    val messageType: MessageType = EmbulkMessageType
      .builder()
      .withSchema(schema)
      .withLogicalTypeHandlers(logicalTypeHandlers)
      .build()
    val metadata: Map[String, String] = Map.empty // NOTE: When is this used?
    new WriteContext(messageType, metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    currentParquetFileWriter = ParquetFileWriter(
      recordConsumer,
      schema,
      timestampFormatters,
      logicalTypeHandlers
    )
  }

  override def write(record: PageReader): Unit = {
    currentParquetFileWriter.write(record)
  }
}
