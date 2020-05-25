package org.embulk.output.s3_parquet.catalog

import java.util.{Optional, Map => JMap}

import com.amazonaws.services.glue.model.{
  Column,
  CreateTableRequest,
  DeleteTableRequest,
  GetTableRequest,
  SerDeInfo,
  StorageDescriptor,
  TableInput
}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.implicits
import org.embulk.spi.{Schema, Column => EmbulkColumn}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object CatalogRegistrator {

  trait Task extends org.embulk.config.Task {
    @Config("catalog_id")
    @ConfigDefault("null")
    def getCatalogId: Optional[String]

    @Config("database")
    def getDatabase: String

    @Config("table")
    def getTable: String

    @Config("column_options")
    @ConfigDefault("{}")
    def getColumnOptions: JMap[String, ColumnOption]

    @Config("operation_if_exists")
    @ConfigDefault("\"delete\"")
    def getOperationIfExists: String
  }

  trait ColumnOption {
    @Config("type")
    def getType: String
  }

  import implicits._

  def fromTask(
      task: CatalogRegistrator.Task,
      aws: Aws,
      schema: Schema,
      location: String,
      compressionCodec: CompressionCodecName,
      defaultGlueTypes: Map[EmbulkColumn, GlueDataType] = Map.empty
  ): CatalogRegistrator =
    CatalogRegistrator(
      aws = aws,
      catalogId = task.getCatalogId,
      database = task.getDatabase,
      table = task.getTable,
      operationIfExists = task.getOperationIfExists,
      location = location,
      compressionCodec = compressionCodec,
      schema = schema,
      columnOptions = task.getColumnOptions,
      defaultGlueTypes = defaultGlueTypes
    )
}

case class CatalogRegistrator(
    aws: Aws,
    catalogId: Option[String] = None,
    database: String,
    table: String,
    operationIfExists: String,
    location: String,
    compressionCodec: CompressionCodecName,
    schema: Schema,
    columnOptions: Map[String, CatalogRegistrator.ColumnOption],
    defaultGlueTypes: Map[EmbulkColumn, GlueDataType] = Map.empty
) {

  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[CatalogRegistrator])

  def run(): Unit = {
    if (doesTableExists()) {
      operationIfExists match {
        case "skip" =>
          logger.info(
            s"Skip to register the table: ${database}.${table}"
          )
          return

        case "delete" =>
          logger.info(s"Delete the table: ${database}.${table}")
          deleteTable()

        case unknown =>
          throw new ConfigException(s"Unsupported operation: $unknown")
      }
    }
    registerNewParquetTable()
    showNewTableInfo()
  }

  def showNewTableInfo(): Unit = {
    val req = new GetTableRequest()
    catalogId.foreach(req.setCatalogId)
    req.setDatabaseName(database)
    req.setName(table)

    val t = aws.withGlue(_.getTable(req)).getTable
    logger.info(s"Created a table: ${t.toString}")
  }

  def doesTableExists(): Boolean = {
    val req = new GetTableRequest()
    catalogId.foreach(req.setCatalogId)
    req.setDatabaseName(database)
    req.setName(table)

    Try(aws.withGlue(_.getTable(req))).isSuccess
  }

  def deleteTable(): Unit = {
    val req = new DeleteTableRequest()
    catalogId.foreach(req.setCatalogId)
    req.setDatabaseName(database)
    req.setName(table)
    aws.withGlue(_.deleteTable(req))
  }

  def registerNewParquetTable(): Unit = {
    logger.info(s"Create a new table: ${database}.${table}")
    val req = new CreateTableRequest()
    catalogId.foreach(req.setCatalogId)
    req.setDatabaseName(database)
    req.setTableInput(
      new TableInput()
        .withName(table)
        .withDescription("Created by embulk-output-s3_parquet")
        .withTableType("EXTERNAL_TABLE")
        .withParameters(
          Map(
            "EXTERNAL" -> "TRUE",
            "classification" -> "parquet",
            "parquet.compression" -> compressionCodec.name()
          )
        )
        .withStorageDescriptor(
          new StorageDescriptor()
            .withColumns(getGlueSchema: _*)
            .withLocation(location)
            .withCompressed(isCompressed)
            .withInputFormat(
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            )
            .withOutputFormat(
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            )
            .withSerdeInfo(
              new SerDeInfo()
                .withSerializationLibrary(
                  "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                )
                .withParameters(Map("serialization.format" -> "1"))
            )
        )
    )
    aws.withGlue(_.createTable(req))
  }

  private def getGlueSchema: Seq[Column] = {
    schema.getColumns.map { c: EmbulkColumn =>
      new Column()
        .withName(c.getName)
        .withType(
          columnOptions
            .get(c.getName)
            .map(_.getType)
            .getOrElse(defaultGlueTypes(c).name)
        )
    }
  }

  private def isCompressed: Boolean = {
    !compressionCodec.equals(CompressionCodecName.UNCOMPRESSED)
  }

}
