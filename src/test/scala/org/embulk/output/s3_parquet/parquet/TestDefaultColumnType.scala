package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.`type`.{
  BooleanType,
  DoubleType,
  JsonType,
  LongType,
  StringType,
  TimestampType
}
import org.embulk.spi.json.JsonParser
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._

class TestDefaultColumnType
    extends AnyFunSuite
    with ParquetColumnTypeTestHelper
    with TableDrivenPropertyChecks
    with Diagrams {

  private val conditions = Table(
    "column",
    Seq(
      SAMPLE_BOOLEAN_COLUMN,
      SAMPLE_LONG_COLUMN,
      SAMPLE_DOUBLE_COLUMN,
      SAMPLE_STRING_COLUMN,
      SAMPLE_TIMESTAMP_COLUMN,
      SAMPLE_JSON_COLUMN
    ): _*
  )

  test(
    "#primitiveType(column) returns PrimitiveTypeName.{BOOLEAN,INT64,DOUBLE,BINARY}"
  ) {
    forAll(conditions) { column =>
      // format: off
      column.getType match {
        case _: BooleanType =>
          assert(PrimitiveTypeName.BOOLEAN == DefaultColumnType.primitiveType(column).getPrimitiveTypeName)
          assert(null == DefaultColumnType.primitiveType(column).getLogicalTypeAnnotation)
        case _: LongType =>
          assert(PrimitiveTypeName.INT64 == DefaultColumnType.primitiveType(column).getPrimitiveTypeName)
          assert(null == DefaultColumnType.primitiveType(column).getLogicalTypeAnnotation)
        case _: DoubleType =>
          assert(PrimitiveTypeName.DOUBLE == DefaultColumnType.primitiveType(column).getPrimitiveTypeName)
          assert(null == DefaultColumnType.primitiveType(column).getLogicalTypeAnnotation)
        case _: StringType | _: TimestampType | _: JsonType =>
          assert(PrimitiveTypeName.BINARY == DefaultColumnType.primitiveType(column).getPrimitiveTypeName)
          assert(LogicalTypeAnnotation.stringType() == DefaultColumnType.primitiveType(column).getLogicalTypeAnnotation)
        case _ =>
          fail()
      }
      // format: on
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { column =>
      // format: off
      column.getType match {
        case _: BooleanType =>
          assert(GlueDataType.BOOLEAN == DefaultColumnType.glueDataType(column))
        case _: LongType =>
          assert(GlueDataType.BIGINT == DefaultColumnType.glueDataType(column))
        case _: DoubleType =>
          assert(GlueDataType.DOUBLE == DefaultColumnType.glueDataType(column))
        case _: StringType | _: TimestampType | _: JsonType =>
          assert(GlueDataType.STRING == DefaultColumnType.glueDataType(column))
        case _ =>
          fail()
      }
      // format: on
    }
  }

  test("#consumeBoolean") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeBoolean(consumer, true)
      }
      assert(consumer.data.head.head.isInstanceOf[Boolean])
      assert(consumer.data.head.head == true)
    }
  }

  test("#consumeString") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeString(consumer, "string")
      }
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString("string"))
    }
  }

  test("#consumeLong") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeLong(consumer, Long.MaxValue)
      }
      assert(consumer.data.head.head.isInstanceOf[Long])
      assert(consumer.data.head.head == Long.MaxValue)
    }
  }

  test("#consumeDouble") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeDouble(consumer, Double.MaxValue)
      }
      assert(consumer.data.head.head.isInstanceOf[Double])
      assert(consumer.data.head.head == Double.MaxValue)
    }
  }

  test("#consumeTimestamp") {
    val formatter = TimestampFormatter
      .of("%Y-%m-%d %H:%M:%S.%6N %z", "UTC")
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeTimestamp(
          consumer,
          Timestamp.ofEpochMilli(Int.MaxValue),
          formatter
        )
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString("1970-01-25 20:31:23.647000 +0000"))
      // format: on
    }
  }

  test("#consumeJson") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DefaultColumnType.consumeJson(
          consumer,
          new JsonParser().parse("""{"a":1,"b":"c","d":5.5,"e":true}""")
        )
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString("""{"a":1,"b":"c","d":5.5,"e":true}"""))
      // format: on
    }
  }
}
