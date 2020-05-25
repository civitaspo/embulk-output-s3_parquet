package org.embulk.output.s3_parquet.catalog

// https://docs.aws.amazon.com/athena/latest/ug/data-types.html

sealed abstract class GlueDataType(val name: String)
object GlueDataType {
  sealed abstract class AbstractIntGlueDataType(name: String, val bitWidth: Int)
      extends GlueDataType(name)

  // BOOLEAN – Values are true and false.
  case object BOOLEAN extends GlueDataType("BOOLEAN")
  // TINYINT – A 8-bit signed INTEGER in two’s complement format, with a minimum value of -27 and a maximum value of 27-1.
  case object TINYINT extends AbstractIntGlueDataType("TINYINT", bitWidth = 8)
  // SMALLINT – A 16-bit signed INTEGER in two’s complement format, with a minimum value of -215 and a maximum value of 215-1.
  case object SMALLINT
      extends AbstractIntGlueDataType("SMALLINT", bitWidth = 16)
  // INT and INTEGER – Athena combines two different implementations of the integer data type, as follows:
  //   * INT – In Data Definition Language (DDL) queries, Athena uses the INT data type.
  //   * INTEGER – In DML queries, Athena uses the INTEGER data type. INTEGER is represented as a 32-bit signed value in two's complement format, with a minimum value of -231 and a maximum value of 231-1.
  case object INT extends AbstractIntGlueDataType("INT", bitWidth = 32)
  // BIGINT – A 64-bit signed INTEGER in two’s complement format, with a minimum value of -263 and a maximum value of 263-1.
  case object BIGINT extends AbstractIntGlueDataType("BIGINT", bitWidth = 64)
  // DOUBLE – A 64-bit double-precision floating point number.
  case object DOUBLE extends GlueDataType("DOUBLE")
  // FLOAT – A 32-bit single-precision floating point number. Equivalent to the REAL in Presto.
  case object FLOAT extends GlueDataType("FLOAT")
  // DECIMAL(precision, scale) – precision is the total number of digits. scale (optional) is the number of digits in fractional part with a default of 0. For example, use these type definitions: DECIMAL(11,5), DECIMAL(15).
  case class DECIMAL(precision: Int, scale: Int)
      extends GlueDataType(s"DECIMAL($precision, $scale)")
  // STRING – A string literal enclosed in single or double quotes. For more information, see STRING Hive Data Type.
  case object STRING extends GlueDataType("STRING")
  // CHAR – Fixed length character data, with a specified length between 1 and 255, such as char(10). For more information, see CHAR Hive Data Type.
  case class CHAR(length: Int) extends GlueDataType(s"CHAR($length)")
  // VARCHAR – Variable length character data, with a specified length between 1 and 65535, such as varchar(10). For more information, see VARCHAR Hive Data Type.
  case class VARCHAR(length: Int) extends GlueDataType(s"VARCHAR($length)")
  // BINARY – Used for data in Parquet.
  case object BINARY extends GlueDataType("BINARY")
  // DATE – A date in UNIX format, such as YYYY-MM-DD.
  case object DATE extends GlueDataType("DATE")
  // TIMESTAMP – Date and time instant in the UNiX format, such as yyyy-mm-dd hh:mm:ss[.f...]. For example, TIMESTAMP '2008-09-15 03:04:05.324'. This format uses the session time zone.
  case object TIMESTAMP extends GlueDataType("TIMESTAMP")
  // ARRAY<data_type>
  case class ARRAY(dataType: GlueDataType)
      extends GlueDataType(s"ARRAY<${dataType.name}>")
  // MAP<primitive_type, data_type>
  case class MAP(keyDataType: GlueDataType, valueDataType: GlueDataType)
      extends GlueDataType(s"MAP<${keyDataType.name}, ${valueDataType.name}>")
  // STRUCT<col_name : data_type [COMMENT col_comment] , ...>
  case class STRUCT(struct: Map[String, GlueDataType])
      extends GlueDataType({
        val columns = struct
          .map {
            case (columnName, glueType) => s"$columnName : ${glueType.name}"
          }
        s"STRUCT<${columns.mkString(", ")}>"
      })
}
