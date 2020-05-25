package org.embulk.output.s3_parquet.parquet

import java.time.ZoneId

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.{
  MICROS,
  MILLIS,
  NANOS
}
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
import org.slf4j.{Logger, LoggerFactory}

case class TimestampLogicalType(
    isAdjustedToUtc: Boolean,
    timeUnit: TimeUnit,
    timeZone: ZoneId
) extends ParquetColumnType {
  private val logger: Logger =
    LoggerFactory.getLogger(classOf[TimestampLogicalType])

  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: LongType | _: TimestampType =>
        Types
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.timestampType(isAdjustedToUtc, timeUnit))
          .named(column.getName)
      case _: BooleanType | _: DoubleType | _: StringType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: LongType | _: TimestampType =>
        timeUnit match {
          case MILLIS => GlueDataType.TIMESTAMP
          case MICROS | NANOS =>
            warningWhenConvertingTimestampToGlueType(GlueDataType.BIGINT)
            GlueDataType.BIGINT
        }
      case _: BooleanType | _: DoubleType | _: StringType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    throw newUnsupportedMethodException("consumeBoolean")
  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    throw newUnsupportedMethodException("consumeString")

  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    consumer.addLong(v)

  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    throw newUnsupportedMethodException("consumeDouble")

  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = timeUnit match {
    case MILLIS => consumer.addLong(v.toEpochMilli)
    case MICROS =>
      consumer.addLong(v.getEpochSecond * 1_000_000L + (v.getNano / 1_000L))
    case NANOS =>
      consumer.addLong(v.getEpochSecond * 1_000_000_000L + v.getNano)
  }

  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    throw newUnsupportedMethodException("consumeJson")

  private def warningWhenConvertingTimestampToGlueType(
      glueType: GlueDataType
  ): Unit =
    logger.warn(
      s"timestamp(isAdjustedToUtc = $isAdjustedToUtc, timeUnit = $timeUnit) is converted" +
        s" to Glue ${glueType.name} but this is not represented correctly, because Glue" +
        s" does not support time type. Please use `catalog.column_options` to define the type."
    )
}
