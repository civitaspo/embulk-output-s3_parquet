package org.embulk.output.s3_parquet.parquet

import org.embulk.spi.Column
import org.embulk.spi.`type`.Types

trait ParquetColumnTypeTestHelper {

  val SAMPLE_BOOLEAN_COLUMN: Column = new Column(0, "a", Types.BOOLEAN)
  val SAMPLE_LONG_COLUMN: Column = new Column(0, "a", Types.LONG)
  val SAMPLE_DOUBLE_COLUMN: Column = new Column(0, "a", Types.DOUBLE)
  val SAMPLE_STRING_COLUMN: Column = new Column(0, "a", Types.STRING)
  val SAMPLE_TIMESTAMP_COLUMN: Column = new Column(0, "a", Types.TIMESTAMP)
  val SAMPLE_JSON_COLUMN: Column = new Column(0, "a", Types.JSON)

  def newMockRecordConsumer(): MockParquetRecordConsumer =
    MockParquetRecordConsumer()
}
