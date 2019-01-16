package org.embulk.output.s3_parquet


import java.io.File
import java.nio.file.{Files, Paths}

import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.amazonaws.services.s3.transfer.model.UploadResult
import org.apache.parquet.hadoop.ParquetWriter
import org.embulk.config.TaskReport
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.spi.{Exec, Page, PageReader, TransactionalPageOutput}

case class S3ParquetPageOutput(outputLocalFile: String,
                               reader: PageReader,
                               writer: ParquetWriter[PageReader],
                               aws: Aws,
                               destBucket: String,
                               destKey: String)
  extends TransactionalPageOutput {

  override def add(page: Page): Unit = {
    reader.setPage(page)
    while (reader.nextRecord()) {
      writer.write(reader)
    }
  }

  override def finish(): Unit = {
  }

  override def close(): Unit = {
    writer.close()
  }

  override def abort(): Unit = {
    cleanup()
  }

  override def commit(): TaskReport = {
    val result: UploadResult = aws.withTransferManager { xfer: TransferManager =>
      val upload: Upload = xfer.upload(destBucket, destKey, new File(outputLocalFile))
      upload.waitForUploadResult()
    }
    cleanup()
    Exec.newTaskReport()
      .set("bucket", result.getBucketName)
      .set("key", result.getKey)
      .set("etag", result.getETag)
      .set("version_id", result.getVersionId)
  }

  private def cleanup(): Unit = {
    Files.delete(Paths.get(outputLocalFile))
  }
}
