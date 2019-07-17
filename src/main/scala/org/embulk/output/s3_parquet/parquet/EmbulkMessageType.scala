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
                       schema: Schema = Schema.builder().build())
    {

        def withName(name: String): Builder =
        {
            Builder(name = name, schema = schema)
        }

        def withSchema(schema: Schema): Builder =
        {
            Builder(name = name, schema = schema)
        }

        def build(): MessageType =
        {
            val builder: ImmutableList.Builder[Type] = ImmutableList.builder[Type]()
            schema.visitColumns(EmbulkMessageTypeColumnVisitor(builder))
            new MessageType("embulk", builder.build())

        }

    }

    private case class EmbulkMessageTypeColumnVisitor(builder: ImmutableList.Builder[Type])
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
            // TODO: Support OriginalType.TIME* ?
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, column.getName, OriginalType.UTF8))
        }

        override def jsonColumn(column: Column): Unit =
        {
            // TODO: does this work?
            builder.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, column.getName, OriginalType.UTF8))
        }
    }

}