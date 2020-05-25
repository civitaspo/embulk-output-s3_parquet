package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.DataException
import org.embulk.spi.time.Timestamp
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._

class TestDateLogicalType
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

  private val unsupportedEmbulkColumns = Seq(
    SAMPLE_BOOLEAN_COLUMN,
    SAMPLE_DOUBLE_COLUMN,
    SAMPLE_STRING_COLUMN,
    SAMPLE_JSON_COLUMN
  )

  test(
    "#primitiveType(column) returns PrimitiveTypeName.INT32 with LogicalType"
  ) {
    forAll(conditions) { column =>
      whenever(!unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(PrimitiveTypeName.INT32 == DateLogicalType.primitiveType(column).getPrimitiveTypeName)
        assert(LogicalTypeAnnotation.dateType() == DateLogicalType.primitiveType(column).getLogicalTypeAnnotation)
        // format: on
      }
    }
  }

  test(
    s"#primitiveType(column) cannot return any PrimitiveType when embulk column type is one of (${unsupportedEmbulkColumns
      .map(_.getType.getName)
      .mkString(",")})"
  ) {
    forAll(conditions) { column =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.primitiveType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { column =>
      whenever(!unsupportedEmbulkColumns.contains(column)) {
        assert(GlueDataType.DATE == DateLogicalType.glueDataType(column))
      }
    }
  }

  test(
    s"#glueDataType(column) cannot return any GlueDataType when embulk column type is one of (${unsupportedEmbulkColumns
      .map(_.getType.getName)
      .mkString(",")})"
  ) {
    forAll(conditions) { column =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.glueDataType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }
    }
  }

  test("#consumeBoolean") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.consumeBoolean(consumer, true)).getMessage.endsWith("is unsupported."))
        // format: on
      }
    }
  }

  test("#consumeString") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.consumeString(consumer, "")).getMessage.endsWith("is unsupported."))
        // format: on
      }
    }
  }

  test("#consumeLong") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DateLogicalType.consumeLong(consumer, 1L)
      }
      assert(consumer.data.head.head.isInstanceOf[Int])
      assert(consumer.data.head.head == 1)
    }
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[DataException](DateLogicalType.consumeLong(consumer, Long.MaxValue)).getMessage.startsWith("Failed to cast Long: "))
        // format: on
      }
    }
  }

  test("#consumeDouble") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.consumeDouble(consumer, 0.0d)).getMessage.endsWith("is unsupported."))
        // format: on
      }

    }
  }

  test("#consumeTimestamp") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        DateLogicalType.consumeTimestamp(
          consumer,
          Timestamp.ofEpochSecond(24 * 60 * 60), // 1day
          null
        )
      }
      assert(consumer.data.head.head.isInstanceOf[Int])
      assert(consumer.data.head.head == 1)
    }
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // NOTE: See. java.time.Instant#MAX_SECOND
        val instantMaxEpochSeconds = 31556889864403199L
        // format: off
        assert(intercept[DataException](DateLogicalType.consumeTimestamp(consumer, Timestamp.ofEpochSecond(instantMaxEpochSeconds), null)).getMessage.startsWith("Failed to cast Long: "))
        // format: on
      }
    }
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // NOTE: See. java.time.Instant#MIN_SECOND
        val instantMinEpochSeconds = -31557014167219200L
        // format: off
        assert(intercept[DataException](DateLogicalType.consumeTimestamp(consumer, Timestamp.ofEpochSecond(instantMinEpochSeconds), null)).getMessage.startsWith("Failed to cast Long: "))
        // format: on
      }
    }
  }

  test("#consumeJson") {
    newMockRecordConsumer().tap { consumer =>
      consumer.writingSampleField {
        // format: off
        assert(intercept[ConfigException](DateLogicalType.consumeJson(consumer, null)).getMessage.endsWith("is unsupported."))
        // format: on
      }
    }
  }
}
