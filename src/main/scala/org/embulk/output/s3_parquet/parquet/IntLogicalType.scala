package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.output.s3_parquet.catalog.GlueDataType.AbstractIntGlueDataType
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
import org.slf4j.{Logger, LoggerFactory}

import scala.math.BigDecimal.RoundingMode

case class IntLogicalType(bitWidth: Int, isSigned: Boolean)
    extends ParquetColumnType {
  require(
    Seq(8, 16, 32, 64).contains(bitWidth),
    s"bitWidth value must be one of (8, 16, 32, 64)."
  )

  private val logger: Logger = LoggerFactory.getLogger(classOf[IntLogicalType])

  private val SIGNED_64BIT_INT_MAX_VALUE = BigInt("9223372036854775807")
  private val SIGNED_64BIT_INT_MIN_VALUE = BigInt("-9223372036854775808")
  private val SIGNED_32BIT_INT_MAX_VALUE = BigInt("2147483647")
  private val SIGNED_32BIT_INT_MIN_VALUE = BigInt("-2147483648")
  private val SIGNED_16BIT_INT_MAX_VALUE = BigInt("32767")
  private val SIGNED_16BIT_INT_MIN_VALUE = BigInt("-32768")
  private val SIGNED_8BIT_INT_MAX_VALUE = BigInt("127")
  private val SIGNED_8BIT_INT_MIN_VALUE = BigInt("-128")
  private val UNSIGNED_64BIT_INT_MAX_VALUE = BigInt("18446744073709551615")
  private val UNSIGNED_64BIT_INT_MIN_VALUE = BigInt("0")
  private val UNSIGNED_32BIT_INT_MAX_VALUE = BigInt("4294967295")
  private val UNSIGNED_32BIT_INT_MIN_VALUE = BigInt("0")
  private val UNSIGNED_16BIT_INT_MAX_VALUE = BigInt("65535")
  private val UNSIGNED_16BIT_INT_MIN_VALUE = BigInt("0")
  private val UNSIGNED_8BIT_INT_MAX_VALUE = BigInt("255")
  private val UNSIGNED_8BIT_INT_MIN_VALUE = BigInt("0")

  private def isINT32: Boolean = bitWidth < 64

  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: BooleanType | _: LongType | _: DoubleType | _: StringType =>
        Types
          .optional(
            if (isINT32) PrimitiveTypeName.INT32
            else PrimitiveTypeName.INT64
          )
          .as(LogicalTypeAnnotation.intType(bitWidth, isSigned))
          .named(column.getName)
      case _: TimestampType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: BooleanType | _: LongType | _: DoubleType | _: StringType =>
        (bitWidth, isSigned) match {
          case (8, true)  => GlueDataType.TINYINT
          case (16, true) => GlueDataType.SMALLINT
          case (32, true) => GlueDataType.INT
          case (64, true) => GlueDataType.BIGINT
          case (8, false) =>
            warningWhenConvertingUnsignedIntegerToGlueType(
              GlueDataType.SMALLINT
            )
            GlueDataType.SMALLINT
          case (16, false) =>
            warningWhenConvertingUnsignedIntegerToGlueType(GlueDataType.INT)
            GlueDataType.INT
          case (32, false) =>
            warningWhenConvertingUnsignedIntegerToGlueType(GlueDataType.BIGINT)
            GlueDataType.BIGINT
          case (64, false) =>
            warningWhenConvertingUnsignedIntegerToGlueType(GlueDataType.BIGINT)
            GlueDataType.BIGINT
          case (_, _) =>
            throw new ConfigException(
              s"Unsupported column type: ${column.getName} (bitWidth: $bitWidth, isSigned: $isSigned)"
            )
        }
      case _: TimestampType | _: JsonType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    if (isINT32)
      consumer.addInteger(
        if (v) 1
        else 0
      )
    else
      consumer.addLong(
        if (v) 1
        else 0
      )

  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    try consumeBigDecimal(consumer, BigDecimal.exact(v))
    catch {
      case ex: NumberFormatException =>
        throw new DataException(s"Failed to cast String: $v to BigDecimal.", ex)
    }
  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    consumeBigInt(consumer, BigInt(v))
  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    consumeBigDecimal(consumer, BigDecimal.exact(v))
  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = throw newUnsupportedMethodException("consumeTimestamp")
  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    throw newUnsupportedMethodException("consumeJson")

  private def warningWhenConvertingUnsignedIntegerToGlueType(
      glueType: AbstractIntGlueDataType
  ): Unit = {
    logger.warn {
      s"int(bit_width = $bitWidth, is_signed  $isSigned) is converted to Glue ${glueType.name}" +
        s" but this is not represented correctly, because the Glue ${glueType.name} represents" +
        s" a ${glueType.bitWidth}-bit signed integer. Please use `catalog.column_options` to define the type."
    }
  }

  private def consumeBigDecimal(consumer: RecordConsumer, v: BigDecimal): Unit =
    // TODO: Make RoundingMode configurable?
    consumeBigInt(consumer, v.setScale(0, RoundingMode.HALF_UP).toBigInt)

  private def consumeBigInt(consumer: RecordConsumer, v: BigInt): Unit = {
    def consume(min: BigInt, max: BigInt): Unit =
      if (min <= v && v <= max)
        if (isINT32) consumer.addInteger(v.toInt)
        else consumer.addLong(v.toLong)
      else
        throw new DataException(
          s"The value is out of the range: that is '$min <= value <= $max'" +
            s" in the case of int(bit_width = $bitWidth, is_signed  $isSigned)" +
            s", but the value is $v."
        )
    (bitWidth, isSigned) match {
      case (8, true) =>
        consume(SIGNED_8BIT_INT_MIN_VALUE, SIGNED_8BIT_INT_MAX_VALUE)
      case (16, true) =>
        consume(SIGNED_16BIT_INT_MIN_VALUE, SIGNED_16BIT_INT_MAX_VALUE)
      case (32, true) =>
        consume(SIGNED_32BIT_INT_MIN_VALUE, SIGNED_32BIT_INT_MAX_VALUE)
      case (64, true) =>
        consume(SIGNED_64BIT_INT_MIN_VALUE, SIGNED_64BIT_INT_MAX_VALUE)
      case (8, false) =>
        consume(UNSIGNED_8BIT_INT_MIN_VALUE, UNSIGNED_8BIT_INT_MAX_VALUE)
      case (16, false) =>
        consume(UNSIGNED_16BIT_INT_MIN_VALUE, UNSIGNED_16BIT_INT_MAX_VALUE)
      case (32, false) =>
        consume(UNSIGNED_32BIT_INT_MIN_VALUE, UNSIGNED_32BIT_INT_MAX_VALUE)
      case (64, false) =>
        consume(UNSIGNED_64BIT_INT_MIN_VALUE, UNSIGNED_64BIT_INT_MAX_VALUE)
      case _ =>
        throw new ConfigException(
          s"int(bit_width = $bitWidth, is_signed  $isSigned) is unsupported."
        )
    }
  }
}
