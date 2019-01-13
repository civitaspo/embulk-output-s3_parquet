package org.embulk.output


import org.embulk.spi.Exec
import org.slf4j.Logger

package object s3_parquet {

  lazy val logger: Logger = Exec.getLogger(classOf[S3ParquetOutputPlugin])

  def withPluginContextClassLoader[A](f: => A): A = {
    val original: ClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(classOf[S3ParquetOutputPlugin].getClassLoader)
    try f
    finally Thread.currentThread.setContextClassLoader(original)
  }

}
