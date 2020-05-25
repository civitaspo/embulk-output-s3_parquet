package org.embulk.output.s3_parquet.parquet

import java.time.{Duration, Instant}

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.`type`.{
  BooleanType,
  DoubleType,
  JsonType,
  LongType,
  StringType,
  TimestampType
}
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.embulk.spi.Column
import org.msgpack.value.Value

object DateLogicalType extends ParquetColumnType {
  override def primitiveType(column: Column): PrimitiveType = {
    column.getType match {
      case _: LongType | _: TimestampType =>
        Types
          .optional(PrimitiveTypeName.INT32)
          .as(LogicalTypeAnnotation.dateType())
          .named(column.getName)
      case _: BooleanType | _: DoubleType | _: StringType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }
  }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: LongType | _: TimestampType => GlueDataType.DATE
      case _: BooleanType | _: DoubleType | _: StringType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    throw newUnsupportedMethodException("consumeBoolean")

  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    throw newUnsupportedMethodException("consumeString")

  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    consumeLongAsInteger(consumer, v)

  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    throw newUnsupportedMethodException("consumeDouble")

  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit =
    consumeLongAsInteger(
      consumer,
      Duration.between(Instant.EPOCH, v.getInstant).toDays
    )

  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    throw newUnsupportedMethodException("consumeJson")
}
