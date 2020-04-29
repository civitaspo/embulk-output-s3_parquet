package org.embulk.output.s3_parquet

import org.embulk.spi.Schema
import org.embulk.spi.`type`.Types
import org.embulk.spi.time.{Timestamp, TimestampFormatter, TimestampParser}
import org.msgpack.value.Value

class TestS3ParquetOutputPlugin extends EmbulkPluginTestHelper {

  test("minimal default case") {
    val schema: Schema = Schema
      .builder()
      .add("c0", Types.BOOLEAN)
      .add("c1", Types.LONG)
      .add("c2", Types.DOUBLE)
      .add("c3", Types.STRING)
      .add("c4", Types.TIMESTAMP)
      .add("c5", Types.JSON)
      .build()
    // scalafmt: { maxColumn = 200 }
    val parser = TimestampParser.of("%Y-%m-%d %H:%M:%S.%N %z", "UTC")
    val data: Seq[Seq[Any]] = Seq(
      Seq(true, 0L, 0.0d, "c212c89f91", parser.parse("2017-10-22 19:53:31.000000 +0900"), newJson(Map("a" -> 0, "b" -> "00"))),
      Seq(false, 1L, -0.5d, "aaaaa", parser.parse("2017-10-22 19:53:31.000000 +0900"), newJson(Map("a" -> 1, "b" -> "11"))),
      Seq(false, 2L, 1.5d, "90823c6a1f", parser.parse("2017-10-23 23:42:43.000000 +0900"), newJson(Map("a" -> 2, "b" -> "22"))),
      Seq(true, 3L, 0.44d, "", parser.parse("2017-10-22 06:12:13.000000 +0900"), newJson(Map("a" -> 3, "b" -> "33", "c" -> 3.3))),
      Seq(false, 9999L, 10000.33333d, "e56a40571c", parser.parse("2017-10-23 04:59:16.000000 +0900"), newJson(Map("a" -> 4, "b" -> "44", "c" -> 4.4, "d" -> true)))
    )
    // scalafmt: { maxColumn = 80 }

    val result: Seq[Seq[AnyRef]] = runOutput(newDefaultConfig, schema, data)

    assert(result.size == 5)
    data.indices.foreach { i =>
      data(i).indices.foreach { j =>
        data(i)(j) match {
          case timestamp: Timestamp =>
            val formatter =
              TimestampFormatter.of("%Y-%m-%d %H:%M:%S.%6N %z", "Asia/Tokyo")
            assert(
              formatter.format(timestamp) == result(i)(j),
              s"A different timestamp value is found (Record Index: $i, Column Index: $j)"
            )
          case value: Value =>
            assert(
              value.toJson == result(i)(j),
              s"A different json value is found (Record Index: $i, Column Index: $j)"
            )
          case _ =>
            assert(
              data(i)(j) == result(i)(j),
              s"A different value is found (Record Index: $i, Column Index: $j)"
            )
        }
      }
    }
  }
}
