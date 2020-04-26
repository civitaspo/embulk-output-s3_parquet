package org.embulk.output.s3_parquet.parquet

import java.util.Optional

import com.google.common.base.{Optional => GOptional}
import org.embulk.config.{ConfigException, TaskSource}
import org.embulk.output.s3_parquet.S3ParquetOutputPlugin.{
  ColumnOptionTask,
  TypeOptionTask
}
import org.embulk.spi.`type`.{Types, Type => EType}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._
import scala.util.Try

class TestLogicalTypeHandlerStore extends AnyFunSuite {
  test("empty() returns empty maps") {
    val rv = LogicalTypeHandlerStore.empty

    assert(rv.fromColumnName.isEmpty)
    assert(rv.fromEmbulkType.isEmpty)
  }

  test("fromEmbulkOptions() returns handlers for valid option tasks") {
    val typeOpts = Map[String, TypeOptionTask](
      "timestamp" -> DummyTypeOptionTask(
        Optional.of[String]("timestamp-millis")
      )
    ).asJava
    val columnOpts = Map[String, ColumnOptionTask](
      "col1" -> DummyColumnOptionTask(Optional.of[String]("timestamp-micros"))
    ).asJava

    val expected1 = Map[EType, LogicalTypeHandler](
      Types.TIMESTAMP -> TimestampMillisLogicalTypeHandler
    )
    val expected2 = Map[String, LogicalTypeHandler](
      "col1" -> TimestampMicrosLogicalTypeHandler
    )

    val rv = LogicalTypeHandlerStore.fromEmbulkOptions(typeOpts, columnOpts)

    assert(rv.fromEmbulkType == expected1)
    assert(rv.fromColumnName == expected2)
  }

  test(
    "fromEmbulkOptions() raises ConfigException if invalid option tasks given"
  ) {
    val emptyTypeOpts = Map.empty[String, TypeOptionTask].asJava
    val emptyColumnOpts = Map.empty[String, ColumnOptionTask].asJava

    val invalidTypeOpts = Map[String, TypeOptionTask](
      "unknown-embulk-type-name" -> DummyTypeOptionTask(
        Optional.of[String]("timestamp-millis")
      ),
      "timestamp" -> DummyTypeOptionTask(
        Optional.of[String]("unknown-parquet-logical-type-name")
      )
    ).asJava
    val invalidColumnOpts = Map[String, ColumnOptionTask](
      "col1" -> DummyColumnOptionTask(
        Optional.of[String]("unknown-parquet-logical-type-name")
      )
    ).asJava

    val try1 = Try(
      LogicalTypeHandlerStore
        .fromEmbulkOptions(invalidTypeOpts, emptyColumnOpts)
    )
    assert(try1.isFailure)
    assert(try1.failed.get.isInstanceOf[ConfigException])

    val try2 = Try(
      LogicalTypeHandlerStore
        .fromEmbulkOptions(emptyTypeOpts, invalidColumnOpts)
    )
    assert(try2.isFailure)
    assert(try2.failed.get.isInstanceOf[ConfigException])

    val try3 = Try(
      LogicalTypeHandlerStore
        .fromEmbulkOptions(invalidTypeOpts, invalidColumnOpts)
    )
    assert(try3.isFailure)
    assert(try3.failed.get.isInstanceOf[ConfigException])
  }

  test("get() returns a handler matched with primary column name condition") {
    val typeOpts = Map[String, TypeOptionTask](
      "timestamp" -> DummyTypeOptionTask(
        Optional.of[String]("timestamp-millis")
      )
    ).asJava
    val columnOpts = Map[String, ColumnOptionTask](
      "col1" -> DummyColumnOptionTask(Optional.of[String]("timestamp-micros"))
    ).asJava

    val handlers =
      LogicalTypeHandlerStore.fromEmbulkOptions(typeOpts, columnOpts)

    // It matches both of column name and embulk type, and column name should be primary
    val expected = Some(TimestampMicrosLogicalTypeHandler)
    val actual = handlers.get("col1", Types.TIMESTAMP)

    assert(actual == expected)
  }

  test("get() returns a handler matched with type name condition") {
    val typeOpts = Map[String, TypeOptionTask](
      "timestamp" -> DummyTypeOptionTask(
        Optional.of[String]("timestamp-millis")
      )
    ).asJava
    val columnOpts = Map.empty[String, ColumnOptionTask].asJava

    val handlers =
      LogicalTypeHandlerStore.fromEmbulkOptions(typeOpts, columnOpts)

    // It matches column name
    val expected = Some(TimestampMillisLogicalTypeHandler)
    val actual = handlers.get("col1", Types.TIMESTAMP)

    assert(actual == expected)
  }

  test("get() returns None if not matched") {
    val typeOpts = Map.empty[String, TypeOptionTask].asJava
    val columnOpts = Map.empty[String, ColumnOptionTask].asJava

    val handlers =
      LogicalTypeHandlerStore.fromEmbulkOptions(typeOpts, columnOpts)

    // It matches embulk type
    val actual = handlers.get("col1", Types.TIMESTAMP)

    assert(actual.isEmpty)
  }

  private case class DummyTypeOptionTask(lt: Optional[String])
      extends TypeOptionTask {

    override def getLogicalType: Optional[String] = {
      lt
    }

    override def validate(): Unit = {}

    override def dump(): TaskSource = {
      null
    }
  }

  private case class DummyColumnOptionTask(lt: Optional[String])
      extends ColumnOptionTask {

    override def getTimeZoneId: GOptional[String] = {
      GOptional.absent[String]
    }

    override def getFormat: GOptional[String] = {
      GOptional.absent[String]
    }

    override def getLogicalType: Optional[String] = {
      lt
    }

    override def validate(): Unit = {}

    override def dump(): TaskSource = {
      null
    }
  }
}
