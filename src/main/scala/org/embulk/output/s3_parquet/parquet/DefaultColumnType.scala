package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.embulk.spi.Column
import org.embulk.spi.`type`.{
  BooleanType,
  DoubleType,
  JsonType,
  LongType,
  StringType,
  TimestampType
}
import org.msgpack.value.Value

object DefaultColumnType extends ParquetColumnType {
  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: BooleanType =>
        Types.optional(PrimitiveTypeName.BOOLEAN).named(column.getName)
      case _: LongType =>
        Types.optional(PrimitiveTypeName.INT64).named(column.getName)
      case _: DoubleType =>
        Types.optional(PrimitiveTypeName.DOUBLE).named(column.getName)
      case _: StringType =>
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      case _: TimestampType =>
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      case _: JsonType =>
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      case _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: BooleanType =>
        GlueDataType.BOOLEAN
      case _: LongType =>
        GlueDataType.BIGINT
      case _: DoubleType =>
        GlueDataType.DOUBLE
      case _: StringType | _: TimestampType | _: JsonType =>
        GlueDataType.STRING
      case _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    consumer.addBoolean(v)
  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    consumer.addBinary(Binary.fromString(v))
  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    consumer.addLong(v)
  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    consumer.addDouble(v)
  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = consumer.addBinary(Binary.fromString(formatter.format(v)))
  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    consumer.addBinary(Binary.fromString(v.toJson))
}
