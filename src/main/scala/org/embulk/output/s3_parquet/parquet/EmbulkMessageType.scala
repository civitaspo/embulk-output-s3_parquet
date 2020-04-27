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

    override def booleanColumn(column: Column): Unit = {
      builder.add(
        Types.optional(PrimitiveTypeName.BOOLEAN).named(column.getName)
      )
    }

    override def longColumn(column: Column): Unit = {
      val name = column.getName
      val et = column.getType

      val t = logicalTypeHandlers.get(name, et) match {
        case Some(h) if h.isConvertible(et) => h.newSchemaFieldType(name)
        case _ =>
          Types.optional(PrimitiveTypeName.INT64).named(column.getName)
      }

      builder.add(t)
    }

    override def doubleColumn(column: Column): Unit = {
      builder.add(
        Types.optional(PrimitiveTypeName.DOUBLE).named(column.getName)
      )
    }

    override def stringColumn(column: Column): Unit = {
      builder.add(
        Types
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named(column.getName)
      )
    }

    override def timestampColumn(column: Column): Unit = {
      val name = column.getName
      val et = column.getType

      val t = logicalTypeHandlers.get(name, et) match {
        case Some(h) if h.isConvertible(et) => h.newSchemaFieldType(name)
        case _ =>
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name)
      }

      builder.add(t)
    }

    override def jsonColumn(column: Column): Unit = {
      val name = column.getName
      val et = column.getType

      val t = logicalTypeHandlers.get(name, et) match {
        case Some(h) if h.isConvertible(et) => h.newSchemaFieldType(name)
        case _ =>
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name)
      }

      builder.add(t)
    }
  }

}
