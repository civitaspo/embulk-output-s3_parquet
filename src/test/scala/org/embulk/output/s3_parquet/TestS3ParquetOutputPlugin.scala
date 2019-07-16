package org.embulk.output.s3_parquet


import java.io.{File, PrintWriter}
import java.nio.file.{FileSystems, Path}

import cloud.localstack.{DockerTestUtils, Localstack, TestUtils}
import cloud.localstack.docker.LocalstackDocker
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.google.common.io.Resources
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import org.embulk.config.ConfigSource
import org.embulk.spi.OutputPlugin
import org.embulk.test.{EmbulkTests, TestingEmbulk}
import org.junit.Rule
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, DiagrammedAssertions, FunSuite}
import org.scalatest.junit.JUnitRunner

import scala.annotation.meta.getter
import scala.jdk.CollectionConverters._

@RunWith(classOf[JUnitRunner])
class TestS3ParquetOutputPlugin
  extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with DiagrammedAssertions {

  val RESOURCE_NAME_PREFIX: String = "org/embulk/output/s3_parquet/"
  val BUCKET_NAME: String = "my-bucket"

  val LOCALSTACK_DOCKER: LocalstackDocker = LocalstackDocker.INSTANCE

  override protected def beforeAll(): Unit = {
    Localstack.teardownInfrastructure()
    LOCALSTACK_DOCKER.startup(LocalstackDockerConfiguration.DEFAULT)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    LOCALSTACK_DOCKER.stop()
    super.afterAll()
  }

  @(Rule@getter)
  val embulk: TestingEmbulk = TestingEmbulk.builder()
    .registerPlugin(classOf[OutputPlugin], "s3_parquet", classOf[S3ParquetOutputPlugin])
    .build()

  before {
    DockerTestUtils.getClientS3.createBucket(BUCKET_NAME)
  }

  def defaultOutConfig(): ConfigSource = {
    embulk.newConfig()
      .set("type", "s3_parquet")
      .set("endpoint", "http://localhost:4572") // See https://github.com/localstack/localstack#overview
      .set("bucket", BUCKET_NAME)
      .set("path_prefix", "path/to/p")
      .set("auth_method", "basic")
      .set("access_key_id", TestUtils.TEST_ACCESS_KEY)
      .set("secret_access_key", TestUtils.TEST_SECRET_KEY)
      .set("path_style_access_enabled", true)
      .set("default_timezone", "Asia/Tokyo")
  }


  test("first test") {
    val inPath = toPath("in1.csv")
    val outConfig = defaultOutConfig()

    val result: TestingEmbulk.RunResult = embulk.runOutput(outConfig, inPath)


    val outRecords: Seq[Map[String, String]] = result.getOutputTaskReports.asScala.map { tr =>
      val b = tr.get(classOf[String], "bucket")
      val k = tr.get(classOf[String], "key")
      readParquetFile(b, k)
    }.foldLeft(Seq[Map[String, String]]()) { (merged,
                                      records) =>
      merged ++ records
    }

    val inRecords: Seq[Seq[String]] = EmbulkTests.readResource(RESOURCE_NAME_PREFIX + "out1.tsv")
      .stripLineEnd
      .split("\n")
      .map(record => record.split("\t").toSeq)
      .toSeq

    inRecords.zipWithIndex.foreach {
      case (record, recordIndex) =>
        0.to(5).foreach { columnIndex =>
          val columnName = s"c$columnIndex"
          val inData: String = inRecords(recordIndex)(columnIndex)
          val outData: String = outRecords(recordIndex).getOrElse(columnName, "")

          assert(outData === inData, s"record: $recordIndex, column: $columnName")
        }
    }
  }

  def readParquetFile(bucket: String,
                      key: String): Seq[Map[String, String]] = {
    val xfer = TransferManagerBuilder.standard()
      .withS3Client(DockerTestUtils.getClientS3)
      .build()
    val createdParquetFile = embulk.createTempFile("in")
    try xfer.download(bucket, key, createdParquetFile.toFile).waitForCompletion()
    finally xfer.shutdownNow()

    val reader: ParquetReader[SimpleRecord] = ParquetReader
      .builder(new SimpleReadSupport(), new HadoopPath(createdParquetFile.toString))
      .build()

    def read(reader: ParquetReader[SimpleRecord],
             records: Seq[Map[String, String]] = Seq()): Seq[Map[String, String]] = {
      val simpleRecord: SimpleRecord = reader.read()
      if (simpleRecord != null) {
        val r: Map[String, String] = simpleRecord.getValues.asScala.map(v => v.getName -> v.getValue.toString).toMap
        return read(reader, records :+ r)
      }
      records
    }

    try read(reader)
    finally {
      reader.close()

    }
  }

  private def toPath(fileName: String) = {
    val url = Resources.getResource(RESOURCE_NAME_PREFIX + fileName)
    FileSystems.getDefault.getPath(new File(url.toURI).getAbsolutePath)
  }

}
