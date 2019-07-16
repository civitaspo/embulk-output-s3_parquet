package org.embulk.output.s3_parquet.parquet


import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.embulk.spi.{PageReader, Schema}
import org.embulk.spi.time.TimestampFormatter

import scala.collection.JavaConverters._


private[parquet] case class ParquetFileWriteSupport(schema: Schema,
                                                    timestampFormatters: Seq[TimestampFormatter])
    extends WriteSupport[PageReader]
{

    private var currentParquetFileWriter: ParquetFileWriter = _

    override def init(configuration: Configuration): WriteContext =
    {
        val messageType: MessageType = EmbulkMessageType.builder()
            .withSchema(schema)
            .build()
        val metadata: Map[String, String] = Map.empty // NOTE: When is this used?
        new WriteContext(messageType, metadata.asJava)
    }

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
    {
        currentParquetFileWriter = ParquetFileWriter(recordConsumer, schema, timestampFormatters)
    }

    override def write(record: PageReader): Unit =
    {
        currentParquetFileWriter.write(record)
    }
}
