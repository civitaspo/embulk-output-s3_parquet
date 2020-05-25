package org.embulk.output.s3_parquet.parquet

import java.time.ZoneId
import java.util.Locale

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS
import org.apache.parquet.schema.PrimitiveType
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.Column
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.msgpack.value.Value

object LogicalTypeProxy {
  private val DEFAULT_SCALE: Int = 0
  private val DEFAULT_BID_WIDTH: Int = 64
  private val DEFAULT_IS_SIGNED: Boolean = true
  private val DEFAULT_IS_ADJUSTED_TO_UTC: Boolean = true
  private val DEFAULT_TIME_UNIT: TimeUnit = MILLIS
  private val DEFAULT_TIME_ZONE: ZoneId = ZoneId.of("UTC")
}

case class LogicalTypeProxy(
    name: String,
    scale: Option[Int] = None,
    precision: Option[Int] = None,
    bitWidth: Option[Int] = None,
    isSigned: Option[Boolean] = None,
    isAdjustedToUtc: Option[Boolean] = None,
    timeUnit: Option[TimeUnit] = None,
    timeZone: Option[ZoneId] = None
) extends ParquetColumnType {
  private def getScale: Int = scale.getOrElse(LogicalTypeProxy.DEFAULT_SCALE)
  private def getPrecision: Int = precision.getOrElse {
    throw new ConfigException("\"precision\" must be set.")
  }
  private def getBidWith: Int =
    bitWidth.getOrElse(LogicalTypeProxy.DEFAULT_BID_WIDTH)
  private def getIsSigned: Boolean =
    isSigned.getOrElse(LogicalTypeProxy.DEFAULT_IS_SIGNED)
  private def getIsAdjustedToUtc: Boolean =
    isAdjustedToUtc.getOrElse(LogicalTypeProxy.DEFAULT_IS_ADJUSTED_TO_UTC)
  private def getTimeUnit: TimeUnit =
    timeUnit.getOrElse(LogicalTypeProxy.DEFAULT_TIME_UNIT)
  private def getTimeZone: ZoneId =
    timeZone.getOrElse(LogicalTypeProxy.DEFAULT_TIME_ZONE)

  lazy val logicalType: ParquetColumnType = {
    name.toUpperCase(Locale.ENGLISH) match {
      case "INT" => IntLogicalType(getBidWith, getIsSigned)
      case "TIMESTAMP" =>
        TimestampLogicalType(getIsAdjustedToUtc, getTimeUnit, getTimeZone)
      case "TIME" =>
        TimeLogicalType(getIsAdjustedToUtc, getTimeUnit, getTimeZone)
      case "DECIMAL" => DecimalLogicalType(getScale, getPrecision)
      case "DATE"    => DateLogicalType
      case "JSON"    => JsonLogicalType
      case _ =>
        throw new ConfigException(s"Unsupported logical_type.name: $name.")
    }
  }

  override def primitiveType(column: Column): PrimitiveType =
    logicalType.primitiveType(column)
  override def glueDataType(column: Column): GlueDataType =
    logicalType.glueDataType(column)
  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    logicalType.consumeBoolean(consumer, v)
  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    logicalType.consumeString(consumer, v)
  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    logicalType.consumeLong(consumer, v)
  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    logicalType.consumeDouble(consumer, v)
  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = logicalType.consumeTimestamp(consumer, v, formatter)
  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    logicalType.consumeJson(consumer, v)
}
