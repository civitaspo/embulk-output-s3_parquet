package org.embulk.output.s3_parquet

import java.io.File
import java.nio.file.FileSystems

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
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
import org.scalatest.{
  BeforeAndAfter,
  BeforeAndAfterAll,
  DiagrammedAssertions,
  FunSuite
}
import org.scalatestplus.junit.JUnitRunner

import scala.annotation.meta.getter
import scala.jdk.CollectionConverters._

@RunWith(classOf[JUnitRunner])
class TestS3ParquetOutputPlugin
    extends FunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with DiagrammedAssertions {

  val RESOURCE_NAME_PREFIX: String = "org/embulk/output/s3_parquet/"
  val TEST_S3_ENDPOINT: String = "http://localhost:4572"
  val TEST_S3_REGION: String = "us-east-1"
  val TEST_S3_ACCESS_KEY_ID: String = "test"
  val TEST_S3_SECRET_ACCESS_KEY: String = "test"
  val TEST_BUCKET_NAME: String = "my-bucket"

  @(Rule @getter)
  val embulk: TestingEmbulk = TestingEmbulk
    .builder()
    .registerPlugin(
      classOf[OutputPlugin],
      "s3_parquet",
      classOf[S3ParquetOutputPlugin]
    )
    .build()

  before {
    withLocalStackS3Client(_.createBucket(TEST_BUCKET_NAME))
  }

  after {
    withLocalStackS3Client(_.deleteBucket(TEST_BUCKET_NAME))
  }

  def defaultOutConfig(): ConfigSource = {
    embulk
      .newConfig()
      .set("type", "s3_parquet")
      .set(
        "endpoint",
        "http://localhost:4572"
      ) // See https://github.com/localstack/localstack#overview
      .set("bucket", TEST_BUCKET_NAME)
      .set("path_prefix", "path/to/p")
      .set("auth_method", "basic")
      .set("access_key_id", TEST_S3_ACCESS_KEY_ID)
      .set("secret_access_key", TEST_S3_SECRET_ACCESS_KEY)
      .set("path_style_access_enabled", true)
      .set("default_timezone", "Asia/Tokyo")
  }

  test("first test") {
    val inPath = toPath("in1.csv")
    val outConfig = defaultOutConfig()

    val result: TestingEmbulk.RunResult = embulk.runOutput(outConfig, inPath)

    val outRecords: Seq[Map[String, String]] =
      result.getOutputTaskReports.asScala
        .map { tr =>
          val b = tr.get(classOf[String], "bucket")
          val k = tr.get(classOf[String], "key")
          readParquetFile(b, k)
        }
        .foldLeft(Seq[Map[String, String]]()) { (merged, records) =>
          merged ++ records
        }

    val inRecords: Seq[Seq[String]] = EmbulkTests
      .readResource(RESOURCE_NAME_PREFIX + "out1.tsv")
      .stripLineEnd
      .split("\n")
      .map(record => record.split("\t").toSeq)
      .toSeq

    inRecords.zipWithIndex.foreach {
      case (record, recordIndex) =>
        0.to(5).foreach { columnIndex =>
          val columnName = s"c$columnIndex"
          val inData: String = inRecords(recordIndex)(columnIndex)
          val outData: String =
            outRecords(recordIndex).getOrElse(columnName, "")

          assert(
            outData === inData,
            s"record: $recordIndex, column: $columnName"
          )
        }
    }
  }

  def readParquetFile(bucket: String, key: String): Seq[Map[String, String]] = {
    val createdParquetFile = embulk.createTempFile("in")
    withLocalStackS3Client { s3 =>
      val xfer = TransferManagerBuilder
        .standard()
        .withS3Client(s3)
        .build()
      try xfer
        .download(bucket, key, createdParquetFile.toFile)
        .waitForCompletion()
      finally xfer.shutdownNow()
    }

    val reader: ParquetReader[SimpleRecord] = ParquetReader
      .builder(
        new SimpleReadSupport(),
        new HadoopPath(createdParquetFile.toString)
      )
      .build()

    def read(
        reader: ParquetReader[SimpleRecord],
        records: Seq[Map[String, String]] = Seq()
    ): Seq[Map[String, String]] = {
      val simpleRecord: SimpleRecord = reader.read()
      if (simpleRecord != null) {
        val r: Map[String, String] = simpleRecord.getValues.asScala
          .map(v => v.getName -> v.getValue.toString)
          .toMap
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

  private def withLocalStackS3Client[A](f: AmazonS3 => A): A = {
    val client: AmazonS3 = AmazonS3ClientBuilder.standard
      .withEndpointConfiguration(
        new EndpointConfiguration(TEST_S3_ENDPOINT, TEST_S3_REGION)
      )
      .withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(
            TEST_S3_ACCESS_KEY_ID,
            TEST_S3_SECRET_ACCESS_KEY
          )
        )
      )
      .withPathStyleAccessEnabled(true)
      .build()

    try f(client)
    finally client.shutdown()
  }
}
