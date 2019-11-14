package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{Type => PType}
import org.apache.parquet.schema.{OriginalType, PrimitiveType}
import org.embulk.spi.DataException
import org.embulk.spi.`type`.{Type => EType}
import org.embulk.spi.`type`.Types
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
    def isConvertible(t: EType): Boolean

    def newSchemaFieldType(name: String): PrimitiveType

    def consume(orig: Any, recordConsumer: RecordConsumer): Unit
}

abstract class IntLogicalTypeHandler(ot: OriginalType) extends LogicalTypeHandler {
    override def isConvertible(t: EType): Boolean = t == Types.LONG

    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(PType.Repetition.OPTIONAL, PrimitiveTypeName.INT64, name, ot)

    override def consume(orig: Any, recordConsumer: RecordConsumer): Unit =
        orig match {
            case v: Long => recordConsumer.addLong(v)
            case _ => throw new DataException("given mismatched type value; expected type is long")
        }
}

object TimestampMillisLogicalTypeHandler extends LogicalTypeHandler {
    override def isConvertible(t: EType): Boolean = t == Types.TIMESTAMP

    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(PType.Repetition.OPTIONAL, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MILLIS)

    override def consume(orig: Any, recordConsumer: RecordConsumer): Unit =
        orig match {
            case ts: Timestamp => recordConsumer.addLong(ts.toEpochMilli)
            case _ => throw new DataException("given mismatched type value; expected type is timestamp")
        }
}

object TimestampMicrosLogicalTypeHandler extends LogicalTypeHandler {
    override def isConvertible(t: EType): Boolean = t == Types.TIMESTAMP

    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(PType.Repetition.OPTIONAL, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MICROS)

    override def consume(orig: Any, recordConsumer: RecordConsumer): Unit =
        orig match {
            case ts: Timestamp =>
                val v = (ts.getEpochSecond * 1_000_000L) + (ts.getNano.asInstanceOf[Long] / 1_000L)
                recordConsumer.addLong(v)
            case _ => throw new DataException("given mismatched type value; expected type is timestamp")
        }
}

object Int8LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.INT_8)
object Int16LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.INT_16)
object Int32LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.INT_32)
object Int64LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.INT_64)

object Uint8LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.UINT_8)
object Uint16LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.UINT_16)
object Uint32LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.UINT_32)
object Uint64LogicalTypeHandler extends IntLogicalTypeHandler(OriginalType.UINT_64)

object JsonLogicalTypeHandler extends LogicalTypeHandler {
    override def isConvertible(t: EType): Boolean = t == Types.JSON

    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(PType.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, name, OriginalType.JSON)

    override def consume(orig: Any, recordConsumer: RecordConsumer): Unit =
        orig match {
            case msgPack: Value =>
                val bin = Binary.fromString(msgPack.toJson)
                recordConsumer.addBinary(bin)
            case _ => throw new DataException("given mismatched type value; expected type is json")
        }
}
