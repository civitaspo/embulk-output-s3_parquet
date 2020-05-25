package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.DataException
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._
class TestIntLogicalType
    extends AnyFunSuite
    with ParquetColumnTypeTestHelper
    with TableDrivenPropertyChecks
    with Diagrams {

  private val conditions = Table(
    ("bitWidth", "isSigned", "column"), {
      for {
        bitWidth <- Seq(8, 16, 32, 64)
        isSigned <- Seq(true, false)
        column <- Seq(
          SAMPLE_BOOLEAN_COLUMN,
          SAMPLE_LONG_COLUMN,
          SAMPLE_DOUBLE_COLUMN,
          SAMPLE_STRING_COLUMN,
          SAMPLE_TIMESTAMP_COLUMN,
          SAMPLE_JSON_COLUMN
        )
      } yield (bitWidth, isSigned, column)
    }: _*
  )

  private val unsupportedEmbulkColumns = Seq(
    SAMPLE_TIMESTAMP_COLUMN,
    SAMPLE_JSON_COLUMN
  )

  private def isINT32(bitWidth: Int): Boolean = bitWidth < 64

  test(
    "#primitiveType(column) returns PrimitiveTypeName.INT32 with LogicalType"
  ) {
    forAll(conditions) { (bitWidth, isSigned, column) =>
      whenever(isINT32(bitWidth) && !unsupportedEmbulkColumns.contains(column)) {
        val logicalType =
          IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
        // format: off
        assert(PrimitiveTypeName.INT32 == logicalType.primitiveType(column).getPrimitiveTypeName)
        assert(LogicalTypeAnnotation.intType(bitWidth, isSigned) == logicalType.primitiveType(column).getLogicalTypeAnnotation)
        // format: on
      }
    }
  }

  test(
    "#primitiveType(column) returns PrimitiveTypeName.INT64 with LogicalType"
  ) {
    forAll(conditions) { (bitWidth, isSigned, column) =>
      whenever(!isINT32(bitWidth) && !unsupportedEmbulkColumns.contains(column)) {
        val logicalType =
          IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
        // format: off
        assert(PrimitiveTypeName.INT64 == logicalType.primitiveType(column).getPrimitiveTypeName)
        assert(LogicalTypeAnnotation.intType(bitWidth, isSigned) == logicalType.primitiveType(column).getLogicalTypeAnnotation)
        // format: on
      }
    }
  }

  test(
    s"#primitiveType(column) cannot return any PrimitiveType when embulk column type is one of (${unsupportedEmbulkColumns
      .map(_.getType.getName)
      .mkString(",")})"
  ) {
    forAll(conditions) { (bitWidth, isSigned, column) =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(intercept[ConfigException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).primitiveType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { (bitWidth, isSigned, column) =>
      whenever(!unsupportedEmbulkColumns.contains(column)) {
        def assertGlueDataType(expected: GlueDataType) = {
          // format: off
          assert(expected == IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).glueDataType(column))
          // format: on
        }
        if (isSigned) {
          bitWidth match {
            case 8  => assertGlueDataType(GlueDataType.TINYINT)
            case 16 => assertGlueDataType(GlueDataType.SMALLINT)
            case 32 => assertGlueDataType(GlueDataType.INT)
            case 64 => assertGlueDataType(GlueDataType.BIGINT)
            case _  => fail()
          }
        }
        else {
          bitWidth match {
            case 8  => assertGlueDataType(GlueDataType.SMALLINT)
            case 16 => assertGlueDataType(GlueDataType.INT)
            case 32 => assertGlueDataType(GlueDataType.BIGINT)
            case 64 => assertGlueDataType(GlueDataType.BIGINT)
            case _  => fail()
          }
        }
      }
    }
  }

  test(
    s"#glueDataType(column) cannot return any GlueDataType when embulk column type is one of (${unsupportedEmbulkColumns
      .map(_.getType.getName)
      .mkString(",")})"
  ) {
    forAll(conditions) { (bitWidth, isSigned, column) =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(intercept[ConfigException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).glueDataType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }
    }
  }

  test("#consumeBoolean (INT32)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeBoolean(consumer, true)
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 1)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeBoolean(consumer, false)
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 0)
        }
      }
    }
  }

  test("#consumeBoolean (INT64)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(!isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeBoolean(consumer, true)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 1L)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeBoolean(consumer, false)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 0L)
        }
      }
    }
  }

  test("#consumeString  (INT32)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeString(consumer, "1")
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 1)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[DataException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeString(consumer, "string")).getMessage.startsWith("Failed to cast String: "))
            // format: on
          }
        }
      }
    }
  }

  test("#consumeString  (INT64)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(!isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeString(consumer, "1")
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 1L)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[DataException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeString(consumer, "string")).getMessage.startsWith("Failed to cast String: "))
            // format: on
          }
        }
      }
    }
  }

  test("#consumeLong (INT32)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeLong(consumer, 1L)
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 1)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[DataException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeLong(consumer, Long.MaxValue)).getMessage.startsWith("The value is out of the range: that is "))
            // format: on
          }
        }
      }
    }
  }

  test("#consumeLong (INT64)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(!isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeLong(consumer, 1L)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 1L)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeLong(consumer, Long.MaxValue)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == Long.MaxValue)
        }
      }
    }
  }

  test("#consumeDouble (INT32)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeDouble(consumer, 1.4d)
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 1)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeDouble(consumer, 1.5d)
          }
          assert(consumer.data.head.head.isInstanceOf[Int])
          assert(consumer.data.head.head == 2)
        }

        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[DataException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeDouble(consumer, Double.MaxValue)).getMessage.startsWith("The value is out of the range: that is "))
            // format: on
          }
        }
      }
    }
  }

  test("#consumeDouble (INT64)") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      whenever(!isINT32(bitWidth)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeDouble(consumer, 1.4d)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 1L)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            IntLogicalType(bitWidth = bitWidth, isSigned = isSigned)
              .consumeDouble(consumer, 1.5d)
          }
          assert(consumer.data.head.head.isInstanceOf[Long])
          assert(consumer.data.head.head == 2L)
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
          assert(intercept[DataException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeDouble(consumer, Double.MaxValue)).getMessage.startsWith("The value is out of the range: "))
          // format: on
          }
        }
      }
    }
  }

  test("#consumeTimestamp is unsupported") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      newMockRecordConsumer().tap { consumer =>
        consumer.writingSampleField {
          // format: off
          assert(intercept[ConfigException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeTimestamp(consumer, null, null)).getMessage.endsWith("is unsupported."))
          // format: on
        }
      }
    }
  }
  test("#consumeJson is unsupported") {
    forAll(conditions) { (bitWidth, isSigned, _) =>
      newMockRecordConsumer().tap { consumer =>
        consumer.writingSampleField {
          // format: off
          assert(intercept[ConfigException](IntLogicalType(bitWidth = bitWidth, isSigned = isSigned).consumeJson(consumer, null)).getMessage.endsWith("is unsupported."))
          // format: on
        }
      }
    }
  }
}
