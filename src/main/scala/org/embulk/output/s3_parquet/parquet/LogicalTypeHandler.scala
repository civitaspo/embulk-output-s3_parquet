package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{OriginalType, PrimitiveType, Type}
import org.embulk.spi.DataException
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
trait LogicalTypeHandler {
    def newSchemaFieldType(name: String): PrimitiveType

    def consume(orig: AnyRef, recordConsumer: RecordConsumer): Unit
}

case class TimestampMillisLogicalTypeHandler() extends LogicalTypeHandler {
    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MILLIS)

    override def consume(orig: AnyRef, recordConsumer: RecordConsumer): Unit =
        orig match {
            case ts: Timestamp => recordConsumer.addLong(ts.toEpochMilli)
            case _ => throw new DataException("given mismatched type value")
        }
}

case class TimestampMicrosLogicalTypeHandler() extends LogicalTypeHandler {
    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MICROS)

    override def consume(orig: AnyRef, recordConsumer: RecordConsumer): Unit =
        orig match {
            case ts: Timestamp =>
                val v = (ts.getEpochSecond * 1_000_000L) + (ts.getNano.asInstanceOf[Long] / 1_000L)
                recordConsumer.addLong(v)
            case _ => throw new DataException("given mismatched type value")
        }
}

case class JsonLogicalTypeHandler() extends LogicalTypeHandler {
    override def newSchemaFieldType(name: String): PrimitiveType =
        new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, name, OriginalType.JSON)

    override def consume(orig: AnyRef, recordConsumer: RecordConsumer): Unit =
        orig match {
            case msgPack: Value =>
                val bin = Binary.fromString(msgPack.toJson)
                recordConsumer.addBinary(bin)
            case _ => throw new DataException("given mismatched type value")
        }
}
