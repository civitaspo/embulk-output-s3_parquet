Embulk::JavaPlugin.register_output(
  "s3_parquet", "org.embulk.output.s3_parquet.S3ParquetOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
