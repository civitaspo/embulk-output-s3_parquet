package org.embulk.output.s3_parquet.parquet
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
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
import org.msgpack.value.{Value, ValueFactory}
import org.slf4j.{Logger, LoggerFactory}

object JsonLogicalType extends ParquetColumnType {
  private val logger: Logger = LoggerFactory.getLogger(JsonLogicalType.getClass)
  override def primitiveType(column: Column): PrimitiveType =
    column.getType match {
      case _: BooleanType | _: LongType | _: DoubleType | _: StringType |
          _: JsonType =>
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.jsonType())
          .named(column.getName)
      case _: TimestampType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def glueDataType(column: Column): GlueDataType =
    column.getType match {
      case _: BooleanType | _: LongType | _: DoubleType | _: StringType |
          _: JsonType =>
        warningWhenConvertingJsonToGlueType(GlueDataType.STRING)
        GlueDataType.STRING
      case _: TimestampType | _ =>
        throw new ConfigException(s"Unsupported column type: ${column.getName}")
    }

  override def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit =
    consumeJson(consumer, ValueFactory.newBoolean(v))

  override def consumeString(consumer: RecordConsumer, v: String): Unit =
    consumeJson(consumer, ValueFactory.newString(v))

  override def consumeLong(consumer: RecordConsumer, v: Long): Unit =
    consumeJson(consumer, ValueFactory.newInteger(v))

  override def consumeDouble(consumer: RecordConsumer, v: Double): Unit =
    consumeJson(consumer, ValueFactory.newFloat(v))

  override def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit = throw newUnsupportedMethodException("consumeTimestamp")

  override def consumeJson(consumer: RecordConsumer, v: Value): Unit =
    consumer.addBinary(Binary.fromString(v.toJson))

  private def warningWhenConvertingJsonToGlueType(
      glueType: GlueDataType
  ): Unit = {
    logger.warn(
      s"json is converted" +
        s" to Glue ${glueType.name} but this is not represented correctly, because Glue" +
        s" does not support json type. Please use `catalog.column_options` to define the type."
    )
  }

}
