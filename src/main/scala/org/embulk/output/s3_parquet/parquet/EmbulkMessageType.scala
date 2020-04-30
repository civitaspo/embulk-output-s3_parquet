package org.embulk.output.s3_parquet.parquet

import com.google.common.collect.ImmutableList
import org.apache.parquet.schema.{
  LogicalTypeAnnotation,
  MessageType,
  Type,
  Types
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.spi.{Column, ColumnVisitor, Schema}

object EmbulkMessageType {

  def builder(): Builder = {
    Builder()
  }

  case class Builder(
      name: String = "embulk",
      schema: Schema = Schema.builder().build(),
      logicalTypeHandlers: LogicalTypeHandlerStore =
        LogicalTypeHandlerStore.empty
  ) {

    def withName(name: String): Builder = copy(name = name)

    def withSchema(schema: Schema): Builder = copy(schema = schema)

    def withLogicalTypeHandlers(
        logicalTypeHandlers: LogicalTypeHandlerStore
    ): Builder = copy(logicalTypeHandlers = logicalTypeHandlers)

    def build(): MessageType = {
      val builder: ImmutableList.Builder[Type] = ImmutableList.builder[Type]()
      schema.visitColumns(
        EmbulkMessageTypeColumnVisitor(builder, logicalTypeHandlers)
      )
      new MessageType("embulk", builder.build())
    }

  }

  private case class EmbulkMessageTypeColumnVisitor(
      builder: ImmutableList.Builder[Type],
      logicalTypeHandlers: LogicalTypeHandlerStore =
        LogicalTypeHandlerStore.empty
  ) extends ColumnVisitor {

    private def addTypeByLogicalTypeHandlerOrDefault(
        column: Column,
        default: => Type
    ): Unit = {
      builder.add(
        logicalTypeHandlers.get(column.getName, column.getType) match {
          case Some(handler) if handler.isConvertible(column.getType) =>
            handler.newSchemaFieldType(column.getName)
          case _ => default
        }
      )
    }

    override def booleanColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types.optional(PrimitiveTypeName.BOOLEAN).named(column.getName)
      })
    }

    override def longColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types.optional(PrimitiveTypeName.INT64).named(column.getName)
      })
    }

    override def doubleColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types.optional(PrimitiveTypeName.DOUBLE).named(column.getName)
      })
    }

    override def stringColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      })
    }

    override def timestampColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      })
    }

    override def jsonColumn(column: Column): Unit = {
      addTypeByLogicalTypeHandlerOrDefault(column, default = {
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      })
    }
  }

}
