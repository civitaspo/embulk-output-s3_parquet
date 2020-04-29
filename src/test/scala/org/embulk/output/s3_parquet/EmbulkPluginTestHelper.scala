package org.embulk.output.s3_parquet

import java.io.File
import java.nio.file.{Files, Path}
import java.util.concurrent.ExecutionException

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.transfer.{
  TransferManager,
  TransferManagerBuilder
}
import com.google.inject.{Binder, Guice, Module, Stage}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import org.embulk.{TestPluginSourceModule, TestUtilityModule}
import org.embulk.config.{
  ConfigLoader,
  ConfigSource,
  DataSourceImpl,
  ModelManager,
  TaskSource
}
import org.embulk.exec.{
  ExecModule,
  ExtensionServiceLoaderModule,
  SystemConfigModule
}
import org.embulk.jruby.JRubyScriptingModule
import org.embulk.plugin.{
  BuiltinPluginSourceModule,
  InjectedPluginSource,
  PluginClassLoaderModule
}
import org.embulk.spi.{Exec, ExecSession, OutputPlugin, PageTestUtils, Schema}
import org.msgpack.value.{Value, ValueFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.diagrams.Diagrams

import scala.jdk.CollectionConverters._
import scala.util.Using

object EmbulkPluginTestHelper {

  case class TestRuntimeModule() extends Module {

    override def configure(binder: Binder): Unit = {
      val systemConfig = new DataSourceImpl(null)
      new SystemConfigModule(systemConfig).configure(binder)
      new ExecModule(systemConfig).configure(binder)
      new ExtensionServiceLoaderModule(systemConfig).configure(binder)
      new BuiltinPluginSourceModule().configure(binder)
      new JRubyScriptingModule(systemConfig).configure(binder)
      new PluginClassLoaderModule().configure(binder)
      new TestUtilityModule().configure(binder)
      new TestPluginSourceModule().configure(binder)
      InjectedPluginSource.registerPluginTo(
        binder,
        classOf[OutputPlugin],
        "s3_parquet",
        classOf[S3ParquetOutputPlugin]
      )
    }
  }

  def getExecSession: ExecSession = {
    val injector =
      Guice.createInjector(Stage.PRODUCTION, TestRuntimeModule())
    val execConfig = new DataSourceImpl(
      injector.getInstance(classOf[ModelManager])
    )
    ExecSession.builder(injector).fromExecConfig(execConfig).build()
  }
}

abstract class EmbulkPluginTestHelper
    extends AnyFunSuite
    with BeforeAndAfter
    with Diagrams {
  private var exec: ExecSession = _

  val TEST_S3_ENDPOINT: String = "http://localhost:4572"
  val TEST_S3_REGION: String = "us-east-1"
  val TEST_S3_ACCESS_KEY_ID: String = "test"
  val TEST_S3_SECRET_ACCESS_KEY: String = "test"
  val TEST_BUCKET_NAME: String = "my-bucket"
  val TEST_PATH_PREFIX: String = "path/to/parquet-"

  before {
    exec = EmbulkPluginTestHelper.getExecSession

    withLocalStackS3Client(_.createBucket(TEST_BUCKET_NAME))
  }
  after {
    exec.cleanup()
    exec = null

    withLocalStackS3Client { cli =>
      @scala.annotation.tailrec
      def rmRecursive(listing: ObjectListing): Unit = {
        listing.getObjectSummaries.asScala.foreach(o =>
          cli.deleteObject(TEST_BUCKET_NAME, o.getKey)
        )
        if (listing.isTruncated)
          rmRecursive(cli.listNextBatchOfObjects(listing))
      }
      rmRecursive(cli.listObjects(TEST_BUCKET_NAME))
    }
    withLocalStackS3Client(_.deleteBucket(TEST_BUCKET_NAME))
  }

  def runOutput(
      outConfig: ConfigSource,
      schema: Schema,
      data: Seq[Seq[Any]]
  ): Seq[Seq[AnyRef]] = {
    try {
      Exec.doWith(
        exec,
        () => {
          val plugin =
            exec.getInjector.getInstance(classOf[S3ParquetOutputPlugin])
          plugin.transaction(
            outConfig,
            schema,
            1,
            (taskSource: TaskSource) => {
              Using.resource(plugin.open(taskSource, schema, 0)) { output =>
                try {
                  PageTestUtils
                    .buildPage(
                      exec.getBufferAllocator,
                      schema,
                      data.flatten: _*
                    )
                    .asScala
                    .foreach(output.add)
                  output.commit()
                }
                catch {
                  case ex: Throwable =>
                    output.abort()
                    throw ex
                }
              }
              Seq.empty.asJava
            }
          )
        }
      )
    }
    catch {
      case ex: ExecutionException => throw ex.getCause
    }

    readS3Parquet(TEST_BUCKET_NAME, TEST_PATH_PREFIX)
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

  def readS3Parquet(bucket: String, prefix: String): Seq[Seq[AnyRef]] = {
    val tmpDir: Path = Files.createTempDirectory("embulk-output-parquet")
    withLocalStackS3Client { s3 =>
      val xfer: TransferManager = TransferManagerBuilder
        .standard()
        .withS3Client(s3)
        .build()
      try xfer
        .downloadDirectory(bucket, prefix, tmpDir.toFile)
        .waitForCompletion()
      finally xfer.shutdownNow()
    }

    def listFiles(file: File): Seq[File] = {
      file
        .listFiles()
        .flatMap(f =>
          if (f.isFile) Seq(f)
          else listFiles(f)
        )
        .toSeq
    }

    listFiles(tmpDir.toFile)
      .map(_.getAbsolutePath)
      .foldLeft(Seq[Seq[AnyRef]]()) {
        (result: Seq[Seq[AnyRef]], path: String) =>
          result ++ readParquetFile(path)
      }
  }

  private def readParquetFile(pathString: String): Seq[Seq[AnyRef]] = {
    val reader: ParquetReader[SimpleRecord] = ParquetReader
      .builder(
        new SimpleReadSupport(),
        new HadoopPath(pathString)
      )
      .build()

    def read(
        reader: ParquetReader[SimpleRecord],
        records: Seq[Seq[AnyRef]] = Seq()
    ): Seq[Seq[AnyRef]] = {
      val simpleRecord: SimpleRecord = reader.read()
      if (simpleRecord != null) {
        val r: Seq[AnyRef] = simpleRecord.getValues.asScala
          .map(_.getValue)
          .toSeq
        return read(reader, records :+ r)
      }
      records
    }
    try read(reader)
    finally reader.close()
  }

  def loadConfigSourceFromYamlString(yaml: String): ConfigSource = {
    new ConfigLoader(exec.getModelManager).fromYamlString(yaml)
  }

  def newJson(map: Map[String, Any]): Value = {
    ValueFactory
      .newMapBuilder()
      .putAll(map.map {
        case (k: String, v: Any) =>
          val value: Value =
            v match {
              case str: String    => ValueFactory.newString(str)
              case bool: Boolean  => ValueFactory.newBoolean(bool)
              case long: Long     => ValueFactory.newInteger(long)
              case int: Int       => ValueFactory.newInteger(int)
              case double: Double => ValueFactory.newFloat(double)
              case float: Float   => ValueFactory.newFloat(float)
              case _              => ValueFactory.newNil()
            }
          ValueFactory.newString(k) -> value
      }.asJava)
      .build()
  }

  def newDefaultConfig: ConfigSource =
    loadConfigSourceFromYamlString(
      s"""
         |endpoint: $TEST_S3_ENDPOINT
         |bucket: $TEST_BUCKET_NAME
         |path_prefix: $TEST_PATH_PREFIX
         |auth_method: basic
         |access_key_id: $TEST_S3_ACCESS_KEY_ID
         |secret_access_key: $TEST_S3_SECRET_ACCESS_KEY
         |path_style_access_enabled: true
         |default_timezone: Asia/Tokyo
         |""".stripMargin
    )
}
