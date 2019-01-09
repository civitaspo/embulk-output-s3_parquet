package org.embulk.output.s3_parquet

import java.util
import com.google.common.base.Optional
import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.Task
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi.Exec
import org.embulk.spi.OutputPlugin
import org.embulk.spi.PageOutput
import org.embulk.spi.Schema
import org.embulk.spi.TransactionalPageOutput

object S3ParquetOutputPlugin {

  trait PluginTask extends Task { // configuration option 1 (required integer)
    @Config("option1") def getOption1: Int
// configuration option 2 (optional string, null is not allowed)
    @Config("option2") @ConfigDefault("\"myvalue\"") def getOption2: String
// configuration option 3 (optional string, null is allowed)
    @Config("option3") @ConfigDefault("null") def getOption3: Optional[String]
  }
}

class S3ParquetOutputPlugin extends OutputPlugin {
  override def transaction(config: ConfigSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff = {
    val task = config.loadConfig(classOf[S3ParquetOutputPlugin.PluginTask])
// retryable (idempotent) output:
// return resume(task.dump(), schema, taskCount, control);
// non-retryable (non-idempotent) output:
    control.run(task.dump)
    Exec.newConfigDiff
  }
  override def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control) =
    throw new UnsupportedOperationException("s3_parquet output plugin does not support resuming")
  override def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: util.List[TaskReport]): Unit = {}
  override def open(taskSource: TaskSource, schema: Schema, taskIndex: Int): TransactionalPageOutput = {
    val task = taskSource.loadTask(classOf[S3ParquetOutputPlugin.PluginTask])
// Write your code here :)
    throw new UnsupportedOperationException("S3ParquetOutputPlugin.run method is not implemented yet")
  }
}
