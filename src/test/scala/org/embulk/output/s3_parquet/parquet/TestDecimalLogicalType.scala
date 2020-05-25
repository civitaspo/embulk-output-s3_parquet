package org.embulk.output.s3_parquet.parquet

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.`type`.{DoubleType, LongType, StringType}
import org.embulk.spi.DataException
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._

class TestDecimalLogicalType
    extends AnyFunSuite
    with ParquetColumnTypeTestHelper
    with TableDrivenPropertyChecks
    with Diagrams {

  private val conditions = Table(
    ("precision", "scale", "column"), {
      for {
        precision <- Seq(1, 9, 10, 18, 19)
        scale <- Seq(0, 1, 20)
        column <- Seq(
          SAMPLE_BOOLEAN_COLUMN,
          SAMPLE_LONG_COLUMN,
          SAMPLE_DOUBLE_COLUMN,
          SAMPLE_STRING_COLUMN,
          SAMPLE_TIMESTAMP_COLUMN,
          SAMPLE_JSON_COLUMN
        )
      } yield (precision, scale, column)
    }: _*
  )

  private val unsupportedEmbulkColumns = Seq(
    SAMPLE_BOOLEAN_COLUMN,
    SAMPLE_TIMESTAMP_COLUMN,
    SAMPLE_JSON_COLUMN
  )

  def isValidScaleAndPrecision(scale: Int, precision: Int): Boolean =
    scale >= 0 && scale < precision && precision > 0

  test("throws IllegalArgumentException") {
    // format: off
    assert(intercept[IllegalArgumentException](DecimalLogicalType(-1, 5)).getMessage.startsWith("requirement failed: Scale must be zero or a positive integer."))
    assert(intercept[IllegalArgumentException](DecimalLogicalType(10, 5)).getMessage.startsWith("requirement failed: Scale must be a positive integer less than the precision."))
    // format: on
  }

  test(
    "#primitiveType(column) returns PrimitiveTypeName.{INT32, INT64, BINARY} with LogicalType"
  ) {
    forAll(conditions) { (precision, scale, column) =>
      whenever(isValidScaleAndPrecision(scale, precision)) {
        // format: off
        column.getType match {
          case _: LongType if 1 <= precision && precision <= 9   =>
            assert(PrimitiveTypeName.INT32 == DecimalLogicalType(scale, precision).primitiveType(column).getPrimitiveTypeName)
            assert(LogicalTypeAnnotation.decimalType(scale, precision) == DecimalLogicalType(scale, precision).primitiveType(column).getLogicalTypeAnnotation)
          case _: LongType if 10 <= precision && precision <= 18 =>
            assert(PrimitiveTypeName.INT64 == DecimalLogicalType(scale, precision).primitiveType(column).getPrimitiveTypeName)
            assert(LogicalTypeAnnotation.decimalType(scale, precision) == DecimalLogicalType(scale, precision).primitiveType(column).getLogicalTypeAnnotation)
          case _: StringType | _: DoubleType                     =>
            assert(PrimitiveTypeName.BINARY == DecimalLogicalType(scale, precision).primitiveType(column).getPrimitiveTypeName)
            assert(LogicalTypeAnnotation.decimalType(scale, precision) == DecimalLogicalType(scale, precision).primitiveType(column).getLogicalTypeAnnotation)
          case _                                                 =>
            assert(intercept[ConfigException](DecimalLogicalType(scale, precision).primitiveType(column)).getMessage.startsWith("Unsupported column type: "))
        }
        // format: on
      }
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { (precision, scale, column) =>
      whenever(isValidScaleAndPrecision(scale, precision)) {
        // format: off
        column.getType match {
          case _: LongType | _: StringType | _: DoubleType =>
            assert(GlueDataType.DECIMAL(precision, scale) == DecimalLogicalType(scale, precision).glueDataType(column))
          case _ =>
            assert(intercept[ConfigException](DecimalLogicalType(scale, precision).glueDataType(column)).getMessage.startsWith("Unsupported column type: "))
        }
        // format: on
      }
    }
  }

  test("#consumeString") {
    forAll(conditions) { (precision, scale, _) =>
      whenever(isValidScaleAndPrecision(scale, precision)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[DataException](DecimalLogicalType(scale, precision).consumeString(consumer, "string")).getMessage.startsWith("Failed to cast String: "))
            // format: on
          }
        }
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            DecimalLogicalType(scale, precision).consumeString(consumer, "5.5")
          }
          assert(consumer.data.head.head.isInstanceOf[Binary])
          if (scale == 0)
            assert(consumer.data.head.head == Binary.fromString("6"))
          else assert(consumer.data.head.head == Binary.fromString("5.5"))
        }
      }
    }
  }

  test("#consumeLong") {
    forAll(conditions) { (precision, scale, _) =>
      whenever(isValidScaleAndPrecision(scale, precision) && precision <= 18) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            DecimalLogicalType(scale, precision)
              .consumeLong(consumer, 1L)
          }
          if (1 <= precision && precision <= 9) {
            assert(consumer.data.head.head.isInstanceOf[Int])
            assert(consumer.data.head.head == 1)
          }
          else {
            assert(consumer.data.head.head.isInstanceOf[Long])
            assert(consumer.data.head.head == 1)
          }
        }
      }
      whenever(isValidScaleAndPrecision(scale, precision) && precision > 18) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            // format: off
            assert(intercept[ConfigException](DecimalLogicalType(scale, precision).consumeLong(consumer, 1L)).getMessage.startsWith("precision must be 1 <= precision <= 18 when consuming long values but precision is "))
            // format: on
          }
        }
      }
    }
  }

  test("#consumeDouble") {
    forAll(conditions) { (precision, scale, _) =>
      whenever(isValidScaleAndPrecision(scale, precision)) {
        newMockRecordConsumer().tap { consumer =>
          consumer.writingSampleField {
            DecimalLogicalType(scale, precision)
              .consumeDouble(consumer, 1.1d)
          }
          assert(consumer.data.head.head.isInstanceOf[Binary])
          if (scale == 0)
            assert(consumer.data.head.head == Binary.fromString("1"))
          else assert(consumer.data.head.head == Binary.fromString("1.1"))
        }
      }
    }
  }

  test("#consume{Boolean,Timestamp,Json} are unsupported.") {
    def assertUnsupportedConsume(f: RecordConsumer => Unit) =
      newMockRecordConsumer().tap { consumer =>
        consumer.writingSampleField {
          // format: off
          assert(intercept[ConfigException](f(consumer)).getMessage.endsWith("is unsupported."))
          // format: on
        }
      }
    assertUnsupportedConsume(DecimalLogicalType(5, 10).consumeBoolean(_, true))
    assertUnsupportedConsume(
      DecimalLogicalType(5, 10).consumeTimestamp(_, null, null)
    )
    assertUnsupportedConsume(DecimalLogicalType(5, 10).consumeJson(_, null))
  }
}
