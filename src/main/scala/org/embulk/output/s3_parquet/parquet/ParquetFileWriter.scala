package org.embulk.output.s3_parquet.parquet


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.embulk.spi.{Column, ColumnVisitor, PageReader, Schema}
import org.embulk.spi.time.TimestampFormatter


object ParquetFileWriter
{

    case class Builder(path: Path = null,
                       schema: Schema = null,
                       timestampFormatters: Seq[TimestampFormatter] = null)
        extends ParquetWriter.Builder[PageReader, Builder](path)
    {

      def withPath(path: Path): Builder =
      {
        copy(path = path)
      }

      def withPath(pathString: String): Builder =
      {
        copy(path = new Path(pathString))
      }

      def withSchema(schema: Schema): Builder =
      {
        copy(schema = schema)
      }

      def withTimestampFormatters(timestampFormatters: Seq[TimestampFormatter]): Builder =
      {
        copy(timestampFormatters = timestampFormatters)
      }

      override def self(): Builder =
      {
        this
      }

        override def getWriteSupport(conf: Configuration): WriteSupport[PageReader] =
        {
            ParquetFileWriteSupport(schema, timestampFormatters)
        }
    }

  def builder(): Builder =
  {
    Builder()
  }

}


private[parquet] case class ParquetFileWriter(recordConsumer: RecordConsumer,
                                              schema: Schema,
                                              timestampFormatters: Seq[TimestampFormatter])
{

    def write(record: PageReader): Unit =
    {
        recordConsumer.startMessage()
        writeRecord(record)
        recordConsumer.endMessage()
    }

    private def writeRecord(record: PageReader): Unit =
    {

        schema.visitColumns(new ColumnVisitor()
        {

            override def booleanColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        recordConsumer.addBoolean(record.getBoolean(column))
                    })
                })
            }

            override def longColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        recordConsumer.addLong(record.getLong(column))
                    })
                })
            }

            override def doubleColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        recordConsumer.addDouble(record.getDouble(column))
                    })
                })
            }

            override def stringColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        val bin = Binary.fromString(record.getString(column))
                        recordConsumer.addBinary(bin)
                    })
                })
            }

            override def timestampColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        // TODO: is a correct way to convert for parquet ?
                        val t = record.getTimestamp(column)
                        val ft = timestampFormatters(column.getIndex).format(t)
                        val bin = Binary.fromString(ft)
                        recordConsumer.addBinary(bin)
                    })
                })
            }

            override def jsonColumn(column: Column): Unit =
            {
                nullOr(column, {
                    withWriteFieldContext(column, {
                        // TODO: is a correct way to convert for parquet ?
                        val msgPack = record.getJson(column)
                        val bin = Binary.fromString(msgPack.toJson)
                        recordConsumer.addBinary(bin)
                    })
                })
            }

            private def nullOr(column: Column,
                               f: => Unit): Unit =
            {
                if (!record.isNull(column)) f
            }

            private def withWriteFieldContext(column: Column,
                                              f: => Unit): Unit =
            {
                recordConsumer.startField(column.getName, column.getIndex)
                f
                recordConsumer.endField(column.getName, column.getIndex)
            }

        })

    }

}