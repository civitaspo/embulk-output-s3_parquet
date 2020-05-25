package org.embulk.output.s3_parquet.parquet

import java.time.ZoneId

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.{
  MICROS,
  MILLIS,
  NANOS
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.spi.DataException
import org.embulk.spi.time.Timestamp
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.chaining._

class TestTimeLogicalType
    extends AnyFunSuite
    with ParquetColumnTypeTestHelper
    with TableDrivenPropertyChecks
    with Diagrams {

  private val conditions = Table(
    ("isAdjustedToUtc", "timeUnit", "timeZone", "column"), {
      for {
        isAdjustedToUtc <- Seq(true, false)
        timeUnit <- Seq(MILLIS, MICROS, NANOS)
        timeZone <- Seq(ZoneId.of("UTC"), ZoneId.of("Asia/Tokyo"))
        column <- Seq(
          SAMPLE_BOOLEAN_COLUMN,
          SAMPLE_LONG_COLUMN,
          SAMPLE_DOUBLE_COLUMN,
          SAMPLE_STRING_COLUMN,
          SAMPLE_TIMESTAMP_COLUMN,
          SAMPLE_JSON_COLUMN
        )
      } yield (isAdjustedToUtc, timeUnit, timeZone, column)
    }: _*
  )

  private val unsupportedEmbulkColumns = Seq(
    SAMPLE_BOOLEAN_COLUMN,
    SAMPLE_DOUBLE_COLUMN,
    SAMPLE_STRING_COLUMN,
    SAMPLE_JSON_COLUMN
  )

  test(
    "#primitiveType(column) returns PrimitiveTypeName.{INT32,INT64} with LogicalType"
  ) {
    forAll(conditions) { (isAdjustedToUtc, timeUnit, timeZone, column) =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
      assert(intercept[ConfigException](TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone = timeZone).primitiveType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }

      whenever(!unsupportedEmbulkColumns.contains(column)) {
        val expectedPrimitiveTypeName =
          if (timeUnit === MILLIS) PrimitiveTypeName.INT32
          else PrimitiveTypeName.INT64
        // format: off
        assert(expectedPrimitiveTypeName == TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone = timeZone).primitiveType(column).getPrimitiveTypeName)
        assert(LogicalTypeAnnotation.timeType(isAdjustedToUtc, timeUnit) == TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone = timeZone).primitiveType(column).getLogicalTypeAnnotation)
        // format: on
      }
    }
  }

  test("#glueDataType(column) returns GlueDataType") {
    forAll(conditions) { (isAdjustedToUtc, timeUnit, timeZone, column) =>
      whenever(unsupportedEmbulkColumns.contains(column)) {
        // format: off
        assert(intercept[ConfigException](TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone = timeZone).glueDataType(column)).getMessage.startsWith("Unsupported column type: "))
        // format: on
      }
      whenever(!unsupportedEmbulkColumns.contains(column)) {
        val expectedGlueDataType =
          if (timeUnit === MILLIS) GlueDataType.INT
          else GlueDataType.BIGINT
        // format: off
        assert(expectedGlueDataType == TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone =  timeZone).glueDataType(column))
        // format: on
      }
    }
  }

  test("#consumeLong") {
    forAll(conditions) { (isAdjustedToUtc, timeUnit, timeZone, _) =>
      timeUnit match {
        case MILLIS =>
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeLong(consumer, 5)
            }
            assert(consumer.data.head.head.isInstanceOf[Int])
            assert(consumer.data.head.head == 5)
          }
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              // format: off
              assert(intercept[DataException](TimeLogicalType(isAdjustedToUtc = isAdjustedToUtc, timeUnit = timeUnit, timeZone = timeZone).consumeLong(consumer, Long.MaxValue)).getMessage.startsWith("Failed to cast Long: "))
              // format: on
            }
          }
        case MICROS | NANOS =>
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeLong(consumer, 5)
            }
            assert(consumer.data.head.head.isInstanceOf[Long])
            assert(consumer.data.head.head == 5L)
          }
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeLong(consumer, Long.MaxValue)
            }
            assert(consumer.data.head.head.isInstanceOf[Long])
            assert(consumer.data.head.head == Long.MaxValue)
          }
      }
    }
  }

  test("#consumeTimestamp") {
    forAll(conditions) { (isAdjustedToUtc, timeUnit, timeZone, _) =>
      timeUnit match {
        case MILLIS =>
          val v = Timestamp.ofEpochMilli(Int.MaxValue)
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeTimestamp(consumer, v, null)
            }
            assert(consumer.data.head.head.isInstanceOf[Int])
            if (timeZone.getId == "Asia/Tokyo" && !isAdjustedToUtc)
              assert(consumer.data.head.head == 19883647)
            else // UTC
              assert(consumer.data.head.head == 73883647)
          }
        case MICROS =>
          val v = Timestamp.ofEpochMilli(Int.MaxValue)
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeTimestamp(consumer, v, null)
            }
            assert(consumer.data.head.head.isInstanceOf[Long])
            if (timeZone.getId == "Asia/Tokyo" && !isAdjustedToUtc)
              assert(consumer.data.head.head == 19883647000L)
            else // UTC
              assert(consumer.data.head.head == 73883647000L)
          }
        case NANOS =>
          val v = Timestamp.ofEpochMilli(Int.MaxValue)
          newMockRecordConsumer().tap { consumer =>
            consumer.writingSampleField {
              TimeLogicalType(
                isAdjustedToUtc = isAdjustedToUtc,
                timeUnit = timeUnit,
                timeZone = timeZone
              ).consumeTimestamp(consumer, v, null)
            }
            assert(consumer.data.head.head.isInstanceOf[Long])
            if (timeZone.getId == "Asia/Tokyo" && !isAdjustedToUtc)
              assert(consumer.data.head.head == 19883647000000L)
            else // UTC
              assert(consumer.data.head.head == 73883647000000L)
          }
      }

    }
  }

  test("#consume{Boolean,Double,String,Json} are unsupported.") {
    def assertUnsupportedConsume(f: RecordConsumer => Unit) =
      newMockRecordConsumer().tap { consumer =>
        consumer.writingSampleField {
          // format: off
          assert(intercept[ConfigException](f(consumer)).getMessage.endsWith("is unsupported."))
          // format: on
        }
      }

    forAll(conditions) { (isAdjustedToUtc, timeUnit, timeZone, _) =>
      val t =
        TimeLogicalType(
          isAdjustedToUtc = isAdjustedToUtc,
          timeUnit = timeUnit,
          timeZone = timeZone
        )
      assertUnsupportedConsume(t.consumeBoolean(_, true))
      assertUnsupportedConsume(t.consumeDouble(_, 0.0d))
      assertUnsupportedConsume(t.consumeString(_, null))
      assertUnsupportedConsume(t.consumeJson(_, null))
    }
  }

}
