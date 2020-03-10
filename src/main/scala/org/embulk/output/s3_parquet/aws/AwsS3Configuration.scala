package org.embulk.output.s3_parquet.aws

import java.util.Optional

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.embulk.config.{Config, ConfigDefault}
import org.embulk.output.s3_parquet.aws.AwsS3Configuration.Task

/*
 * These are advanced settings, so write no documentation.
 */
object AwsS3Configuration {

  trait Task {

    @Config("accelerate_mode_enabled")
    @ConfigDefault("null")
    def getAccelerateModeEnabled: Optional[Boolean]

    @Config("chunked_encoding_disabled")
    @ConfigDefault("null")
    def getChunkedEncodingDisabled: Optional[Boolean]

    @Config("dualstack_enabled")
    @ConfigDefault("null")
    def getDualstackEnabled: Optional[Boolean]

    @Config("force_global_bucket_access_enabled")
    @ConfigDefault("null")
    def getForceGlobalBucketAccessEnabled: Optional[Boolean]

    @Config("path_style_access_enabled")
    @ConfigDefault("null")
    def getPathStyleAccessEnabled: Optional[Boolean]

    @Config("payload_signing_enabled")
    @ConfigDefault("null")
    def getPayloadSigningEnabled: Optional[Boolean]

  }

  def apply(task: Task): AwsS3Configuration = {
    new AwsS3Configuration(task)
  }
}

class AwsS3Configuration(task: Task) {

  def configureAmazonS3ClientBuilder(builder: AmazonS3ClientBuilder): Unit = {
    task.getAccelerateModeEnabled.ifPresent(v =>
      builder.setAccelerateModeEnabled(v)
    )
    task.getChunkedEncodingDisabled.ifPresent(v =>
      builder.setChunkedEncodingDisabled(v)
    )
    task.getDualstackEnabled.ifPresent(v => builder.setDualstackEnabled(v))
    task.getForceGlobalBucketAccessEnabled.ifPresent(v =>
      builder.setForceGlobalBucketAccessEnabled(v)
    )
    task.getPathStyleAccessEnabled.ifPresent(v =>
      builder.setPathStyleAccessEnabled(v)
    )
    task.getPayloadSigningEnabled.ifPresent(v =>
      builder.setPayloadSigningEnabled(v)
    )
  }

}
