package org.embulk.output.s3_parquet.parquet

import com.google.common.collect.ImmutableList
import org.apache.parquet.schema.{MessageType, OriginalType, PrimitiveType, Type}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.embulk.spi.{Column, ColumnVisitor, Schema}


object EmbulkMessageType
{

    def builder(): Builder =
    {
        Builder()
    }

    case class Builder(name: String = "embulk",
                       schema: Schema = Schema.builder().build(),
                       logicalTypeHandlers: LogicalTypeHandlerStore = LogicalTypeHandlerStore.empty)
    {

        def withName(name: String): Builder =
        {
            Builder(name = name, schema = schema, logicalTypeHandlers = logicalTypeHandlers)
        }

        def withSchema(schema: Schema): Builder =
        {
            Builder(name = name, schema = schema, logicalTypeHandlers = logicalTypeHandlers)
        }

        def withLogicalTypeHandlers(logicalTypeHandlers: LogicalTypeHandlerStore): Builder =
        {
            Builder(name = name, schema = schema, logicalTypeHandlers = logicalTypeHandlers)
        }

        def build(): MessageType =
        {
            val builder: ImmutableList.Builder[Type] = ImmutableList.builder[Type]()
            schema.visitColumns(EmbulkMessageTypeColumnVisitor(builder, logicalTypeHandlers))
            new MessageType("embulk", builder.build())
        }

    }

    private case class EmbulkMessageTypeColumnVisitor(builder: ImmutableList.Builder[Type],
                                                      logicalTypeHandlers: LogicalTypeHandlerStore = LogicalTypeHandlerStore.empty)
        extends ColumnVisitor
    {

        override def booleanColumn(column: Column): Unit =
        {
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, column.getName))
        }

        override def longColumn(column: Column): Unit =
        {
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT64, column.getName))
        }

        override def doubleColumn(column: Column): Unit =
        {
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, column.getName))
        }

        override def stringColumn(column: Column): Unit =
        {
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, column.getName, OriginalType.UTF8))
        }

        override def timestampColumn(column: Column): Unit =
        {
            val name = column.getName

            val t = logicalTypeHandlers.get(name, column.getType) match {
                case Some(h) => h.newSchemaFieldType(name)
                case _ => new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, name, OriginalType.UTF8)
            }

            builder.add(t)
        }

        override def jsonColumn(column: Column): Unit =
        {
            val name = column.getName

            val t = logicalTypeHandlers.get(name, column.getType) match {
                case Some(h) => h.newSchemaFieldType(name)
                case _ => new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, name, OriginalType.UTF8)
            }

            builder.add(t)
        }
    }

}