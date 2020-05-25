package org.embulk.output.s3_parquet.parquet

import java.time.{OffsetTime, ZoneId}
import java.time.temporal.ChronoField.{MICRO_OF_DAY, MILLI_OF_DAY, NANO_OF_DAY}

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
import org.embulk.spi.Column
import org.embulk.spi.`type`.{
  BooleanType,
  DoubleType,
  JsonType,
  LongType,
  StringType,
  TimestampType
}
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.msgpack.value.Value
import org.slf4j.{Logger, LoggerFactory}

case class TimeLogicalType(
    isAdjustedToUtc: Boolean,
    timeUnit: TimeUnit,
    timeZone: ZoneId
) extends ParquetColumnType {
  private val logger: Logger = LoggerFactory.getLogger(classOf[TimeLogicalType])
  private val UTC: ZoneId = ZoneId.of("UTC")

  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: LongType | _: TimestampType =>
        Types
          .optional(timeUnit match {
            case MILLIS         => PrimitiveTypeName.INT32
            case MICROS | NANOS => PrimitiveTypeName.INT64
          })
          .as(LogicalTypeAnnotation.timeType(isAdjustedToUtc, timeUnit))
          .named(column.getName)
      case _: BooleanType | _: DoubleType | _: StringType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: LongType | _: TimestampType =>
        timeUnit match {
          case MILLIS =>
            warningWhenConvertingTimeToGlueType(GlueDataType.INT)
            GlueDataType.INT
          case MICROS | NANOS =>
            warningWhenConvertingTimeToGlueType(GlueDataType.BIGINT)
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
    timeUnit match {
      case MILLIS         => consumeLongAsInteger(consumer, v)
      case MICROS | NANOS => consumer.addLong(v)
    }

  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    throw newUnsupportedMethodException("consumeDouble")

  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = {
    // * `TIME` with precision `MILLIS` is used for millisecond precision.
    //   It must annotate an `int32` that stores the number of milliseconds after midnight.
    // * `TIME` with precision `MICROS` is used for microsecond precision.
    //   It must annotate an `int64` that stores the number of microseconds after midnight.
    // * `TIME` with precision `NANOS` is used for nanosecond precision.
    //   It must annotate an `int64` that stores the number of nanoseconds after midnight.
    //
    // ref. https://github.com/apache/parquet-format/blob/apache-parquet-format-2.7.0/LogicalTypes.md#time
    val zoneId = if (isAdjustedToUtc) UTC else timeZone
    val offsetTime: OffsetTime = OffsetTime.ofInstant(v.getInstant, zoneId)
    timeUnit match {
      case MILLIS =>
        consumeLongAsInteger(consumer, offsetTime.get(MILLI_OF_DAY))
      case MICROS =>
        consumer.addLong(offsetTime.getLong(MICRO_OF_DAY))
      case NANOS =>
        consumer.addLong(offsetTime.getLong(NANO_OF_DAY))
    }
  }

  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    throw newUnsupportedMethodException("consumeJson")

  private def warningWhenConvertingTimeToGlueType(
      glueType: GlueDataType
  ): Unit =
    logger.warn(
      s"time(isAdjustedToUtc = $isAdjustedToUtc, timeUnit = $timeUnit) is converted to Glue" +
        s" ${glueType.name} but this is not represented correctly, because Glue does not" +
        s" support time type. Please use `catalog.column_options` to define the type."
    )
}
