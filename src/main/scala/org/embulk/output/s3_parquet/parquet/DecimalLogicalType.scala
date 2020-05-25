package org.embulk.output.s3_parquet.parquet

import java.math.{MathContext, RoundingMode => JRoundingMode}

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.{Column, DataException}
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

import scala.math.BigDecimal.RoundingMode

case class DecimalLogicalType(scale: Int, precision: Int)
    extends ParquetColumnType {
  // ref. https://github.com/apache/parquet-format/blob/apache-parquet-format-2.8.0/LogicalTypes.md#decimal
  require(scale >= 0, "Scale must be zero or a positive integer.")
  require(
    scale < precision,
    "Scale must be a positive integer less than the precision."
  )
  require(
    precision > 0,
    "Precision is required and must be a non-zero positive integer."
  )

  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: LongType if 1 <= precision && precision <= 9 =>
        Types
          .optional(PrimitiveTypeName.INT32)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(column.getName)
      case _: LongType if 10 <= precision && precision <= 18 =>
        Types
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(column.getName)
      case _: StringType | _: DoubleType =>
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(column.getName)
      case _: BooleanType | _: TimestampType | _: JsonType | _ =>
        throw new ConfigException(
          s"Unsupported column type: ${column.getName} (scale: $scale, precision: $precision)"
        )
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: StringType | _: LongType | _: DoubleType =>
        GlueDataType.DECIMAL(scale = scale, precision = precision)
      case _: BooleanType | _: TimestampType | _: JsonType | _ =>
        throw new ConfigException(
          s"Unsupported column type: ${column.getName} (scale: $scale, precision: $precision)"
        )
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    throw newUnsupportedMethodException("consumeBoolean")
  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    try consumeBigDecimal(consumer, BigDecimal.exact(v))
    catch {
      case ex: NumberFormatException =>
        throw new DataException(s"Failed to cast String: $v to BigDecimal.", ex)
    }
  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    if (1 <= precision && precision <= 9) consumeLongAsInteger(consumer, v)
    else if (10 <= precision && precision <= 18) consumer.addLong(v)
    else
      throw new ConfigException(
        s"precision must be 1 <= precision <= 18 when consuming long values but precision is $precision."
      )
  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    consumeBigDecimal(consumer, BigDecimal.exact(v))
  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = throw newUnsupportedMethodException("consumeTimestamp")
  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    throw newUnsupportedMethodException("consumeJson")

  private def consumeBigDecimal(consumer: RecordConsumer, v: BigDecimal): Unit =
    // TODO: Make RoundingMode configurable?
    consumer.addBinary(
      Binary.fromString(
        v.setScale(scale, RoundingMode.HALF_UP)
          .round(new MathContext(precision, JRoundingMode.HALF_UP))
          .toString()
      )
    )
}
