package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.`type`.{
  BooleanType,
  DoubleType,
  JsonType,
  LongType,
  StringType
}
import org.embulk.spi.json.JsonParser
import org.embulk.spi.time.TimestampFormatter
import org.msgpack.value.ValueFactory
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._

class TestJsonLogicalType
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
    "#primitiveType(column) returns PrimitiveTypeName.{BOOLEAN,INT64,DOUBLE,BINARY} with LogicalType"
  ) {
    forAll(conditions) { column =>
      // format: off
      column.getType match {
        case _: BooleanType | _: LongType | _: DoubleType | _: StringType |
            _: JsonType =>
          assert(PrimitiveTypeName.BINARY == JsonLogicalType.primitiveType(column).getPrimitiveTypeName)
          assert(LogicalTypeAnnotation.jsonType() == JsonLogicalType.primitiveType(column).getLogicalTypeAnnotation)
        case _          =>
          assert(intercept[ConfigException](JsonLogicalType.primitiveType(column)).getMessage.startsWith("Unsupported column type: "))
      }
      // format: on
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { column =>
      // format: off
      column.getType match {
        case _: BooleanType | _: LongType | _: DoubleType | _: StringType |
             _: JsonType =>
          assert(GlueDataType.STRING == JsonLogicalType.glueDataType(column))
        case _          =>
          assert(intercept[ConfigException](JsonLogicalType.glueDataType(column)).getMessage.startsWith("Unsupported column type: "))
      }
      // format: on
    }
  }

  test("#consumeBoolean") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        JsonLogicalType.consumeBoolean(consumer, true)
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString(ValueFactory.newBoolean(true).toJson))
      // format: on
    }
  }

  test("#consumeString") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        JsonLogicalType.consumeString(consumer, "string")
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString(ValueFactory.newString("string").toJson))
      // format: on
    }
  }

  test("#consumeLong") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        JsonLogicalType.consumeLong(consumer, Long.MaxValue)
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString(ValueFactory.newInteger(Long.MaxValue).toJson))
      // format: on
    }
  }

  test("#consumeDouble") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        JsonLogicalType.consumeDouble(consumer, Double.MaxValue)
      }
      // format: off
      assert(consumer.data.head.head.isInstanceOf[Binary])
      assert(consumer.data.head.head == Binary.fromString(ValueFactory.newFloat(Double.MaxValue).toJson))
      // format: on
    }
  }

  test("#consumeTimestamp") {
    val formatter = TimestampFormatter
      .of("%Y-%m-%d %H:%M:%S.%6N %z", "UTC")
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[ConfigException](JsonLogicalType.consumeTimestamp(consumer, null, null)).getMessage.endsWith("is unsupported."))
        // format: on
      }
    }
  }

  test("#consumeJson") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        JsonLogicalType.consumeJson(
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
