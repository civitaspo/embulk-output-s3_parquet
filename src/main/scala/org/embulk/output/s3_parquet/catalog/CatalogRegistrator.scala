package org.embulk.output.s3_parquet.catalog


import java.util.{Optional, Map => JMap}

import com.amazonaws.services.athena.model.{ResultConfiguration, StartQueryExecutionRequest}
import com.amazonaws.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, SUCCEEDED}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.catalog.CatalogRegistrator.ColumnOptions
import org.embulk.spi.Schema
import org.embulk.spi.`type`.{BooleanType, DoubleType, JsonType, LongType, StringType, TimestampType, Type}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._


object CatalogRegistrator
{
    trait Task extends org.embulk.config.Task
    {
        @Config("database")
        def getDatabase: String

        @Config("table")
        def getTable: String

        @Config("column_options")
        @ConfigDefault("{}")
        def getColumnOptions: JMap[String, ColumnOptions]

        @Config("drop_if_exists")
        @ConfigDefault("true")
        def getDropIfExists: Boolean

        @Config("workgroup")
        @ConfigDefault("null")
        def getWorkGroup: Optional[String]
    }

    trait ColumnOptions
    {
        @Config("type")
        def getType: String
    }

    def apply(aws: Aws,
              task: Task,
              schema: Schema,
              location: String,
              compressionCodec: CompressionCodecName,
              loggerOption: Option[Logger] = None): CatalogRegistrator =
    {
        new CatalogRegistrator(aws, task, schema, location, compressionCodec, loggerOption)
    }
}

class CatalogRegistrator(aws: Aws,
                         task: CatalogRegistrator.Task,
                         schema: Schema,
                         location: String,
                         compressionCodec: CompressionCodecName,
                         loggerOption: Option[Logger] = None)
{
    val logger: Logger = loggerOption.getOrElse(LoggerFactory.getLogger(classOf[CatalogRegistrator]))

    def run(): Unit =
    {
        if (task.getDropIfExists) dropTableIfExists()
        createParquetTable()
    }

    def createParquetTable(): Unit =
    {
        val indent: String = " " * 4
        val columnOptions: Map[String, ColumnOptions] = task.getColumnOptions.asScala.toMap
        val columnsDDL: String = schema.getColumns.asScala.map { c =>
            val cType: String =
                if (columnOptions.contains(c.getName)) columnOptions(c.getName).getType
                else convertEmbulkType2AthenaType(c.getType)
            indent + s"`${c.getName}`" + cType
        }.mkString(",\n")

        // ref. https://docs.aws.amazon.com/ja_jp/athena/latest/ug/compression-formats.html
        val ddl: String =
            s"""
               | CREATE EXTERNAL TABLE IF NOT EXISTS `${task.getDatabase}`.`${task.getTable}` (
               | $columnsDDL
               | )
               | ROW FORMAT SERDE
               |  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
               | STORED AS INPUTFORMAT
               |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
               | OUTPUTFORMAT
               |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
               | LOCATION
               |  '$location'
               | TBLPROPERTIES (
               |   'classification'='parquet',
               |   'parquet.compression'='${compressionCodec.getParquetCompressionCodec}'
               | )
               |
             """.stripMargin
        runQuery(query = ddl)
    }

    private def dropTableIfExists(): Unit =
    {
        val query = s"DROP TABLE IF EXISTS ${task.getDatabase}.${task.getTable}"
        runQuery(query)
    }

    private def runQuery(query: String): Unit =
    {
        logger.info(s"Run a query: $query")
        val req = new StartQueryExecutionRequest()
        req.setQueryString(query)
        if (task.getWorkGroup.isPresent) req.setWorkGroup(task.getWorkGroup.get())
        else req.setResultConfiguration(new ResultConfiguration().withOutputLocation(null)) // TODO

        val executionId: String = aws.withAthena(_.startQueryExecution(req)).getQueryExecutionId
        newWaiter().wait(executionId)
    }

    private def newWaiter(timeoutSeconds: Long = 5 * 60): AthenaQueryWaiter =
    {
        AthenaQueryWaiter(aws = aws, successStats = Seq(SUCCEEDED), failureStats = Seq(FAILED, CANCELLED), timeoutSeconds = timeoutSeconds)
    }

    private def convertEmbulkType2AthenaType(t: Type): String =
    {
        t match {
            case _: BooleanType   => "BOOLEAN"
            case _: LongType      => "BIGINT"
            case _: DoubleType    => "DOUBLE"
            case _: StringType    => "STRING"
            case _: TimestampType => "STRING"
            case _: JsonType      => "STRING"
            case unknown => throw new ConfigException(s"Unsupported embulk type: ${unknown.getName}")
        }
    }
}
