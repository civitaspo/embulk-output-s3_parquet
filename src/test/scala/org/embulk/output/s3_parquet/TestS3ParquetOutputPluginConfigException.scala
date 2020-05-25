package org.embulk.output.s3_parquet

import org.embulk.config.ConfigException
import org.embulk.spi.Schema
import org.embulk.spi.`type`.Types

class TestS3ParquetOutputPluginConfigException extends EmbulkPluginTestHelper {

  test(
    "Throw ConfigException when un-convertible types are defined in type_options"
  ) {
    val schema = Schema.builder().add("c0", Types.STRING).build()
    val data: Seq[Seq[String]] = Seq(
      Seq("a")
    )
    val cfg = newDefaultConfig.merge(
      loadConfigSourceFromYamlString("""
                                       |type_options:
                                       |  string:
                                       |    logical_type: "timestamp-millis"
                                       |""".stripMargin)
    )
    val caught = intercept[ConfigException](runOutput(cfg, schema, data))
    assert(caught.isInstanceOf[ConfigException])
    assert(caught.getMessage.startsWith("Unsupported column type: "))
  }

  test(
    "Throw ConfigException when un-convertible types are defined in column_options"
  ) {
    val schema = Schema.builder().add("c0", Types.STRING).build()
    val data: Seq[Seq[String]] = Seq(
      Seq("a")
    )
    val cfg = newDefaultConfig.merge(
      loadConfigSourceFromYamlString("""
                                       |column_options:
                                       |  c0:
                                       |    logical_type: "timestamp-millis"
                                       |""".stripMargin)
    )
    val caught = intercept[ConfigException](runOutput(cfg, schema, data))
    assert(caught.isInstanceOf[ConfigException])
    assert(caught.getMessage.startsWith("Unsupported column type: "))

  }

}
