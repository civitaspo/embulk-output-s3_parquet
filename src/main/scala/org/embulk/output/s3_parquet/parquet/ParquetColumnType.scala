package org.embulk.output.s3_parquet.parquet

import java.time.ZoneId
import java.util.{Locale, Optional}

import org.apache.parquet.format.ConvertedType
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.{
  MICROS,
  MILLIS,
  NANOS
}
import org.apache.parquet.schema.PrimitiveType
import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  ConfigSource,
  Task => EmbulkTask
}
import org.embulk.output.s3_parquet.catalog.GlueDataType
import org.embulk.output.s3_parquet.implicits
import org.embulk.spi.{Column, DataException, Exec}
import org.embulk.spi.time.{Timestamp, TimestampFormatter}
import org.embulk.spi.time.TimestampFormatter.TimestampColumnOption
import org.msgpack.value.Value
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}
import scala.util.chaining._

object ParquetColumnType {

  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[ParquetColumnType])

  trait Task extends EmbulkTask with TimestampColumnOption {
    @Config("logical_type")
    @ConfigDefault("null")
    def getLogicalType: Optional[LogicalTypeOption]
  }

  trait LogicalTypeOption extends EmbulkTask {
    @Config("name")
    def getName: String

    @Config("scale")
    @ConfigDefault("null")
    def getScale: Optional[Int]

    @Config("precision")
    @ConfigDefault("null")
    def getPrecision: Optional[Int]

    @Config("bit_width")
    @ConfigDefault("null")
    def getBitWidth: Optional[Int]

    @Config("is_signed")
    @ConfigDefault("null")
    def getIsSigned: Optional[Boolean]

    @Config("is_adjusted_to_utc")
    @ConfigDefault("null")
    def getIsAdjustedToUtc: Optional[Boolean]

    @Config("time_unit")
    @ConfigDefault("null")
    def getTimeUnit: Optional[TimeUnit]
  }

  object LogicalTypeOption {
    case class ConfigBuilder private () {
      case class Attributes private (
          name: Option[String] = None,
          precision: Option[Int] = None,
          scale: Option[Int] = None,
          bitWidth: Option[Int] = None,
          isSigned: Option[Boolean] = None,
          isAdjustedToUtc: Option[Boolean] = None,
          timeUnit: Option[TimeUnit] = None
      ) {
        def toOnelineYaml: String = {
          val builder = Seq.newBuilder[String]
          name.foreach(v => builder.addOne(s"name: ${v}"))
          precision.foreach(v => builder.addOne(s"precision: ${v}"))
          scale.foreach(v => builder.addOne(s"scale: ${v}"))
          bitWidth.foreach(v => builder.addOne(s"bit_width: ${v}"))
          isSigned.foreach(v => builder.addOne(s"is_signed: ${v}"))
          isAdjustedToUtc.foreach(v =>
            builder.addOne(s"is_adjusted_to_utc: ${v}")
          )
          timeUnit.foreach(tu => builder.addOne(s"time_unit: ${tu.name()}"))
          "{" + builder.result().mkString(", ") + "}"
        }

        def build(): ConfigSource = {
          val c = Exec.newConfigSource()
          name.foreach(c.set("name", _))
          precision.foreach(c.set("precision", _))
          scale.foreach(c.set("scale", _))
          bitWidth.foreach(c.set("bit_width", _))
          isSigned.foreach(c.set("is_signed", _))
          isAdjustedToUtc.foreach(c.set("is_adjusted_to_utc", _))
          timeUnit.foreach(tu => c.set("time_unit", tu.name()))
          c
        }
      }
      var attrs: Attributes = Attributes()

      def name(name: String): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(name = Option(name)))
      def scale(scale: Int): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(scale = Option(scale)))
      def precision(precision: Int): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(precision = Option(precision)))
      def bitWidth(bitWidth: Int): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(bitWidth = Option(bitWidth)))
      def isSigned(isSigned: Boolean): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(isSigned = Option(isSigned)))
      def isAdjustedToUtc(isAdjustedToUtc: Boolean): ConfigBuilder =
        this.tap(_ =>
          attrs = attrs.copy(isAdjustedToUtc = Option(isAdjustedToUtc))
        )
      def timeUnit(timeUnit: TimeUnit): ConfigBuilder =
        this.tap(_ => attrs = attrs.copy(timeUnit = Option(timeUnit)))

      def toOnelineYaml: String = attrs.toOnelineYaml

      def build(): ConfigSource = attrs.build()
    }

    def builder(): ConfigBuilder = ConfigBuilder()
  }

  def loadConfig(c: ConfigSource): Task = {
    if (c.has("logical_type")) {
      Try(c.get(classOf[String], "logical_type")).foreach { v =>
        logger.warn(
          "[DEPRECATED] Now, it is deprecated to use the \"logical_type\" option in this usage." +
            " Use \"converted_type\" instead."
        )
        logger.warn(
          s"[DEPRECATED] Translate {logical_type: $v} => {converted_type: $v}"
        )
        c.remove("logical_type")
        c.set("converted_type", v)
      }
    }
    if (c.has("converted_type")) {
      if (c.has("logical_type"))
        throw new ConfigException(
          "\"converted_type\" and \"logical_type\" options cannot be used at the same time."
        )
      Try(c.get(classOf[String], "converted_type")) match {
        case Success(convertedType) =>
          val logicalTypeConfig: ConfigSource =
            translateConvertedType2LogicalType(convertedType)
          c.setNested("logical_type", logicalTypeConfig)
        case Failure(ex) =>
          throw new ConfigException(
            "The value of \"converted_type\" option must be string.",
            ex
          )
      }
    }
    c.loadConfig(classOf[Task])
  }

  private def translateConvertedType2LogicalType(
      convertedType: String
  ): ConfigSource = {
    val builder = LogicalTypeOption.builder()
    val normalizedConvertedType: String = normalizeConvertedType(convertedType)
    if (normalizedConvertedType == "TIMESTAMP_NANOS") {
      builder.name("timestamp").isAdjustedToUtc(true).timeUnit(NANOS)
      logger.warn(
        s"[DEPRECATED] $convertedType is deprecated because this is not one of" +
          s" ConvertedTypes actually. Please use 'logical_type: ${builder.toOnelineYaml}'"
      )
    }
    else {

      ConvertedType.valueOf(normalizedConvertedType) match {
        case ConvertedType.UTF8 => builder.name("string")
        case ConvertedType.DATE => builder.name("date")
        case ConvertedType.TIME_MILLIS =>
          builder.name("time").isAdjustedToUtc(true).timeUnit(MILLIS)
        case ConvertedType.TIME_MICROS =>
          builder.name("time").isAdjustedToUtc(true).timeUnit(MICROS)
        case ConvertedType.TIMESTAMP_MILLIS =>
          builder.name("timestamp").isAdjustedToUtc(true).timeUnit(MILLIS)
        case ConvertedType.TIMESTAMP_MICROS =>
          builder.name("timestamp").isAdjustedToUtc(true).timeUnit(MICROS)
        case ConvertedType.UINT_8 =>
          builder.name("int").bitWidth(8).isSigned(false)
        case ConvertedType.UINT_16 =>
          builder.name("int").bitWidth(16).isSigned(false)
        case ConvertedType.UINT_32 =>
          builder.name("int").bitWidth(32).isSigned(false)
        case ConvertedType.UINT_64 =>
          builder.name("int").bitWidth(64).isSigned(false)
        case ConvertedType.INT_8 =>
          builder.name("int").bitWidth(8).isSigned(true)
        case ConvertedType.INT_16 =>
          builder.name("int").bitWidth(16).isSigned(true)
        case ConvertedType.INT_32 =>
          builder.name("int").bitWidth(32).isSigned(true)
        case ConvertedType.INT_64 =>
          builder.name("int").bitWidth(64).isSigned(true)
        case ConvertedType.JSON => builder.name("json")
        case _                  =>
          // MAP, MAP_KEY_VALUE, LIST, ENUM, DECIMAL, BSON, INTERVAL
          throw new ConfigException(
            s"converted_type: $convertedType is not supported."
          )
      }
    }
    logger.info(
      s"Translate {converted_type: $convertedType} => {logical_type: ${builder.toOnelineYaml}}"
    )
    builder.build()
  }

  private def normalizeConvertedType(convertedType: String): String = {
    convertedType
      .toUpperCase(Locale.ENGLISH)
      .replaceAll("-", "_")
      .replaceAll("INT(\\d)", "INT_$1")
  }

  def fromTask(task: Task): Option[LogicalTypeProxy] = {
    task.getLogicalType.map { o =>
      LogicalTypeProxy(
        name = o.getName,
        scale = o.getScale,
        precision = o.getPrecision,
        bitWidth = o.getBitWidth,
        isSigned = o.getIsSigned,
        isAdjustedToUtc = o.getIsAdjustedToUtc,
        timeUnit = o.getTimeUnit,
        timeZone = task.getTimeZoneId.map(ZoneId.of)
      )
    }
  }
}

trait ParquetColumnType {
  def primitiveType(column: Column): PrimitiveType
  def glueDataType(column: Column): GlueDataType
  def consumeBoolean(consumer: RecordConsumer, v: Boolean): Unit
  def consumeString(consumer: RecordConsumer, v: String): Unit
  def consumeLong(consumer: RecordConsumer, v: Long): Unit
  def consumeDouble(consumer: RecordConsumer, v: Double): Unit
  def consumeTimestamp(
      consumer: RecordConsumer,
      v: Timestamp,
      formatter: TimestampFormatter
  ): Unit
  def consumeJson(consumer: RecordConsumer, v: Value): Unit
  def newUnsupportedMethodException(methodName: String) =
    new ConfigException(s"${getClass.getName}#$methodName is unsupported.")

  protected def consumeLongAsInteger(
      consumer: RecordConsumer,
      v: Long
  ): Unit = {
    if (v < Int.MinValue || v > Int.MaxValue)
      throw new DataException(
        s"Failed to cast Long: $v to Int, " +
          s"because $v exceeds ${Int.MaxValue} (Int.MaxValue) or ${Int.MinValue} (Int.MinValue)"
      )
    consumer.addInteger(v.toInt)
  }
}
