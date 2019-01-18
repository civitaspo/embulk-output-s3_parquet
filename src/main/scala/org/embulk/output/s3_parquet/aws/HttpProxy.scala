package org.embulk.output.s3_parquet.aws


import java.util.Optional

import com.amazonaws.{ClientConfiguration, Protocol}
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.output.s3_parquet.aws.HttpProxy.Task

object HttpProxy {

  trait Task {

    @Config("host")
    @ConfigDefault("null")
    def getHost: Optional[String]

    @Config("port")
    @ConfigDefault("null")
    def getPort: Optional[Int]

    @Config("protocol")
    @ConfigDefault("\"https\"")
    def getProtocol: String

    @Config("user")
    @ConfigDefault("null")
    def getUser: Optional[String]

    @Config("password")
    @ConfigDefault("null")
    def getPassword: Optional[String]

  }

  def apply(task: Task): HttpProxy = new HttpProxy(task)

}

class HttpProxy(task: Task) {

  def configureClientConfiguration(cc: ClientConfiguration): Unit = {
    task.getHost.ifPresent(v => cc.setProxyHost(v))
    task.getPort.ifPresent(v => cc.setProxyPort(v))

    Protocol.values.find(p => p.name().equals(task.getProtocol)) match {
      case Some(v) =>
        cc.setProtocol(v)
      case None =>
        throw new ConfigException(s"'${task.getProtocol}' is unsupported: `protocol` must be one of [${Protocol.values.map(v => s"'$v'").mkString(", ")}].")
    }

    task.getUser.ifPresent(v => cc.setProxyUsername(v))
    task.getPassword.ifPresent(v => cc.setProxyPassword(v))
  }
}
