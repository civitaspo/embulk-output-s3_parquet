package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.spi.DataException
import org.embulk.spi.`type`.{Type => EmbulkType, Types => EmbulkTypes}
import org.embulk.spi.time.Timestamp
import org.msgpack.value.Value

/**
  * Handle Apache Parquet 'Logical Types' on schema/value conversion.
  * ref. https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
  *
  * It focuses on only older representation because newer supported since 1.11 is not used actually yet.
  * TODO Support both of older and newer representation after 1.11+ is published and other middleware supports it.
  *
  */
sealed trait LogicalTypeHandler {
  def isConvertible(t: EmbulkType): Boolean

  def newSchemaFieldType(name: String): PrimitiveType

  def consume(orig: Any, recordConsumer: RecordConsumer): Unit
}

abstract class IntLogicalTypeHandler(logicalType: LogicalTypeAnnotation)
    extends LogicalTypeHandler {

  override def isConvertible(t: EmbulkType): Boolean = {
    t == EmbulkTypes.LONG
  }

  override def newSchemaFieldType(name: String): PrimitiveType = {
    Types.optional(PrimitiveTypeName.INT64).as(logicalType).named(name)
  }

  override def consume(orig: Any, recordConsumer: RecordConsumer): Unit = {
    orig match {
      case v: Long => recordConsumer.addLong(v)
      case _ =>
        throw new DataException(
          "given mismatched type value; expected type is long"
        )
    }
  }
}

object TimestampMillisLogicalTypeHandler extends LogicalTypeHandler {

  override def isConvertible(t: EmbulkType): Boolean = {
    t == EmbulkTypes.TIMESTAMP
  }

  override def newSchemaFieldType(name: String): PrimitiveType = {
    Types
      .optional(PrimitiveTypeName.INT64)
      .as(
        LogicalTypeAnnotation
          .timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
      )
      .named(name)
  }

  override def consume(orig: Any, recordConsumer: RecordConsumer): Unit = {
    orig match {
      case ts: Timestamp => recordConsumer.addLong(ts.toEpochMilli)
      case _ =>
        throw new DataException(
          "given mismatched type value; expected type is timestamp"
        )
    }
  }
}

object TimestampMicrosLogicalTypeHandler extends LogicalTypeHandler {

  override def isConvertible(t: EmbulkType): Boolean = {
    t == EmbulkTypes.TIMESTAMP
  }

  override def newSchemaFieldType(name: String): PrimitiveType = {
    Types
      .optional(PrimitiveTypeName.INT64)
      .as(
        LogicalTypeAnnotation
          .timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)
      )
      .named(name)
  }

  override def consume(orig: Any, recordConsumer: RecordConsumer): Unit = {
    orig match {
      case ts: Timestamp =>
        val v = (ts.getEpochSecond * 1_000_000L) + (ts.getNano
          .asInstanceOf[Long] / 1_000L)
        recordConsumer.addLong(v)
      case _ =>
        throw new DataException(
          "given mismatched type value; expected type is timestamp"
        )
    }
  }
}

object Int8LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(8, true))

object Int16LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(16, true))

object Int32LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(32, true))

object Int64LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(64, true))

object Uint8LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(8, false))

object Uint16LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(16, false))

object Uint32LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(32, false))

object Uint64LogicalTypeHandler
    extends IntLogicalTypeHandler(LogicalTypeAnnotation.intType(64, false))

object JsonLogicalTypeHandler extends LogicalTypeHandler {

  override def isConvertible(t: EmbulkType): Boolean = {
    t == EmbulkTypes.JSON
  }

  override def newSchemaFieldType(name: String): PrimitiveType = {
    Types
      .optional(PrimitiveTypeName.BINARY)
      .as(LogicalTypeAnnotation.jsonType())
      .named(name)
  }

  override def consume(orig: Any, recordConsumer: RecordConsumer): Unit = {
    orig match {
      case msgPack: Value =>
        val bin = Binary.fromString(msgPack.toJson)
        recordConsumer.addBinary(bin)
      case _ =>
        throw new DataException(
          "given mismatched type value; expected type is json"
        )
    }
  }
}
