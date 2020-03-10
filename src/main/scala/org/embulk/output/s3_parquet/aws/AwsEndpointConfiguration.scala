package org.embulk.output.s3_parquet.aws

import java.util.Optional

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.{DefaultAwsRegionProviderChain, Regions}
import org.embulk.config.{Config, ConfigDefault}
import org.embulk.output.s3_parquet.aws.AwsEndpointConfiguration.Task

import scala.util.Try

object AwsEndpointConfiguration {

  trait Task {

    @Config("endpoint")
    @ConfigDefault("null")
    def getEndpoint: Optional[String]

    @Config("region")
    @ConfigDefault("null")
    def getRegion: Optional[String]

  }

  def apply(task: Task): AwsEndpointConfiguration = {
    new AwsEndpointConfiguration(task)
  }
}

class AwsEndpointConfiguration(task: Task) {

  def configureAwsClientBuilder[S <: AwsClientBuilder[S, T], T](
      builder: AwsClientBuilder[S, T]
  ): Unit = {
    if (task.getRegion.isPresent && task.getEndpoint.isPresent) {
      val ec =
        new EndpointConfiguration(task.getEndpoint.get, task.getRegion.get)
      builder.setEndpointConfiguration(ec)
    }
    else if (task.getRegion.isPresent && !task.getEndpoint.isPresent) {
      builder.setRegion(task.getRegion.get)
    }
    else if (!task.getRegion.isPresent && task.getEndpoint.isPresent) {
      val r: String = Try(new DefaultAwsRegionProviderChain().getRegion)
        .getOrElse(Regions.DEFAULT_REGION.getName)
      val e: String = task.getEndpoint.get
      val ec = new EndpointConfiguration(e, r)
      builder.setEndpointConfiguration(ec)
    }
  }

}
