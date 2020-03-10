package org.embulk.output.s3_parquet

// WARNING: This object should be used for limited purposes only.
object ContextClassLoaderSwapper {

  def using[A](klass: Class[_])(f: => A): A = {
    val currentTread = Thread.currentThread()
    val original = currentTread.getContextClassLoader
    val target = klass.getClassLoader
    currentTread.setContextClassLoader(target)
    try f
    finally currentTread.setContextClassLoader(original)
  }

  def usingPluginClass[A](f: => A): A = {
    using(classOf[S3ParquetOutputPlugin])(f)
  }
}
