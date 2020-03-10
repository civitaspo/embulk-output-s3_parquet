package org.embulk.output.s3_parquet.parquet

import org.embulk.spi.`type`.{Type, Types}
import java.util.{Map => JMap}

import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.S3ParquetOutputPlugin.{
  ColumnOptionTask,
  TypeOptionTask
}

import scala.jdk.CollectionConverters._

/**
  * A storage has mapping from logical type query (column name, type) to handler.
  *
  * @param fromEmbulkType
  * @param fromColumnName
  */
case class LogicalTypeHandlerStore private (
    fromEmbulkType: Map[Type, LogicalTypeHandler],
    fromColumnName: Map[String, LogicalTypeHandler]
) {

  // Try column name lookup, then column type
  def get(n: String, t: Type): Option[LogicalTypeHandler] = {
    get(n) match {
      case Some(h) => Some(h)
      case _ =>
        get(t) match {
          case Some(h) => Some(h)
          case _       => None
        }
    }
  }

  def get(t: Type): Option[LogicalTypeHandler] = {
    fromEmbulkType.get(t)
  }

  def get(n: String): Option[LogicalTypeHandler] = {
    fromColumnName.get(n)
  }
}

object LogicalTypeHandlerStore {

  private val STRING_TO_EMBULK_TYPE = Map[String, Type](
    "boolean" -> Types.BOOLEAN,
    "long" -> Types.LONG,
    "double" -> Types.DOUBLE,
    "string" -> Types.STRING,
    "timestamp" -> Types.TIMESTAMP,
    "json" -> Types.JSON
  )

  // Listed only older logical types that we can convert from embulk type
  private val STRING_TO_LOGICAL_TYPE = Map[String, LogicalTypeHandler](
    "timestamp-millis" -> TimestampMillisLogicalTypeHandler,
    "timestamp-micros" -> TimestampMicrosLogicalTypeHandler,
    "int8" -> Int8LogicalTypeHandler,
    "int16" -> Int16LogicalTypeHandler,
    "int32" -> Int32LogicalTypeHandler,
    "int64" -> Int64LogicalTypeHandler,
    "uint8" -> Uint8LogicalTypeHandler,
    "uint16" -> Uint16LogicalTypeHandler,
    "uint32" -> Uint32LogicalTypeHandler,
    "uint64" -> Uint64LogicalTypeHandler,
    "json" -> JsonLogicalTypeHandler
  )

  def empty: LogicalTypeHandlerStore = {
    LogicalTypeHandlerStore(
      Map.empty[Type, LogicalTypeHandler],
      Map.empty[String, LogicalTypeHandler]
    )
  }

  def fromEmbulkOptions(
      typeOpts: JMap[String, TypeOptionTask],
      columnOpts: JMap[String, ColumnOptionTask]
  ): LogicalTypeHandlerStore = {
    val fromEmbulkType = typeOpts.asScala
      .filter(_._2.getLogicalType.isPresent)
      .map[Type, LogicalTypeHandler] {
        case (k, v) =>
          val t = STRING_TO_EMBULK_TYPE.get(k)
          val h = STRING_TO_LOGICAL_TYPE.get(v.getLogicalType.get)
          (t, h) match {
            case (Some(tt), Some(hh)) => (tt, hh)
            case _ =>
              throw new ConfigException("invalid logical types in type_options")
          }
      }
      .toMap

    val fromColumnName = columnOpts.asScala
      .filter(_._2.getLogicalType.isPresent)
      .map[String, LogicalTypeHandler] {
        case (k, v) =>
          val h = STRING_TO_LOGICAL_TYPE.get(v.getLogicalType.get)
          h match {
            case Some(hh) => (k, hh)
            case _ =>
              throw new ConfigException(
                "invalid logical types in column_options"
              )
          }
      }
      .toMap

    LogicalTypeHandlerStore(fromEmbulkType, fromColumnName)
  }
}
