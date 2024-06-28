0.5.3 (2024-06-28)
==================

* [Enhancement] [#55](https://github.com/civitaspo/embulk-output-s3_parquet/pull/55) Replace parquet-tools with parquet-avro
* [Enhancement] [#57](https://github.com/civitaspo/embulk-output-s3_parquet/pull/57) Upgrade hadoop-common library to resolve CVE-2021-37404


0.5.2 (2020-10-12)
==================

* [Fix] [#51](https://github.com/civitaspo/embulk-output-s3_parquet/pull/51) Use PluginClassLoader when oparating catalog.

0.5.1 (2020-06-24)
==================

* [Fix] [#47](https://github.com/civitaspo/embulk-output-s3_parquet/pull/47) Use lower case without any space for Glue data type.

0.5.0 (2020-05-25)
==================

* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Introduce the new usage of **column_options.logical_type**, **type_options.logical_type** to configure more detailed logical types.
* [Deprecated] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) The old usage of **column_options.logical_type**, **type_options.logical_type** is deprecated. Use **column_options.converted_type**, **type_options.converted_type** instead.
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support casting boolean, double, string, timestamp, json to the int logical type.
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support casting long to the timestamp logical type.
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support the decimal logical type. (close [#44](https://github.com/civitaspo/embulk-output-s3_parquet/issues/44))
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support the time logical type.
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support the date logical type.
* [New Feature] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Support is_adjusted_to_utc = false for the timestamp logical type.
* [Fix] [#45](https://github.com/civitaspo/embulk-output-s3_parquet/pull/45) Fix the issue 'Logical type int{8,16,32} don't work' (close [#43](https://github.com/civitaspo/embulk-output-s3_parquet/issues/43))
* [Enhancement] Add lots of tests.

0.4.2 (2020-04-30)
==================

* [Enhancement] [#40](https://github.com/civitaspo/embulk-output-s3_parquet/pull/40) Check combinations with embulk-type and logical-type strictly.

0.4.1 (2020-04-30)
==================

* [Enhancement] [#37](https://github.com/civitaspo/embulk-output-s3_parquet/pull/37) Rewrite the integration tests to make writing and reading tests easier & Use Diagrams for all test cases.
* [Enhancement] [#38](https://github.com/civitaspo/embulk-output-s3_parquet/pull/38) Make all column types enable to use LogicalTypeHandler.
* [Enhancement] [#38](https://github.com/civitaspo/embulk-output-s3_parquet/pull/38) Make parquet schema testable.
* [New Feature] [#38](https://github.com/civitaspo/embulk-output-s3_parquet/pull/38) Support timestamp-nanos.

0.4.0 (2020-04-28)
==================

* [Enhancement] [#35](https://github.com/civitaspo/embulk-output-s3_parquet/pull/35) Fix deprecation warnings.


0.3.0 (2020-04-26)
==================

* [Enhancement] [#27](https://github.com/civitaspo/embulk-output-s3_parquet/pull/27) Github Actions releases automatically when a new release tag pushed instead of releasing from local.
  * [HotFix] [#29](https://github.com/civitaspo/embulk-output-s3_parquet/pull/29) Do not skip the CI when a tag is pushed.
* [Enhancement] [#28](https://github.com/civitaspo/embulk-output-s3_parquet/pull/28) Apply the "org.embulk.embulk-plugins" Gradle plugin.

0.2.0 (2020-03-10)
==================

* [Enhancement] [#23](https://github.com/civitaspo/embulk-output-s3_parquet/pull/23) Limit the usage of swapping ContextClassLoader
* [BugFix] [#24](https://github.com/civitaspo/embulk-output-s3_parquet/pull/24) Use basic credentials correctly
* [Enhancement] [#20](https://github.com/civitaspo/embulk-output-s3_parquet/pull/20) Update gradle 4.1 -> 6.1
* [Enhancement] [#20](https://github.com/civitaspo/embulk-output-s3_parquet/pull/20) Update parquet-{column,common,encoding,hadoop,jackson,tools} 1.10.1 -> 1.11.0 with the latest parquet-format 2.4.0 -> 2.7.0
    * [parquet-format CHANGELOG](https://github.com/apache/parquet-format/blob/master/CHANGES.md)
    * [parquet-mr CHANGELOG](https://github.com/apache/parquet-mr/blob/apache-parquet-1.11.0/CHANGES.md#version-1110)
* [Enhancement] [#20](https://github.com/civitaspo/embulk-output-s3_parquet/pull/20) Update aws-java-sdk 1.11.676 -> 1.11.739
* [Enhancement] [#20](https://github.com/civitaspo/embulk-output-s3_parquet/pull/20) Update embulk 0.9.20 -> 0.9.23 with embulk-deps-{config,buffer}
* [Enhancement] [#19](https://github.com/civitaspo/embulk-output-s3_parquet/pull/19) Use scalafmt instead of the Intellij formatter.
* [Enhancement] [#19](https://github.com/civitaspo/embulk-output-s3_parquet/pull/19) Use scalafmt in CI.
* [Enhancement] [#19](https://github.com/civitaspo/embulk-output-s3_parquet/pull/19) Enable to run examples locally with some prepared scripts.

0.1.0 (2019-11-17)
==================

* [New Feature] Support Logical Types older representations(OriginalTypes) #12 
* [Enhancement] Add Github Actions CI settings #13 
* [Enhancement] Support LogicalTypes for Glue Data Catalog #14 
* [Enhancement] Update dependencies #15
* [New Feature] Support `auth_method: web_identity_token` #15 

0.0.3 (2019-07-17)
==================

* [New Feature] Add `catalog` option to register a new table that has data created by `s3_parquet` plugin.
* [Enhancement] Update dependencies.

0.0.2 (2019-01-21)
==================

* [Fix] Close local buffer files before uploading even if lots of pages exist.

0.0.1 (2019-01-18)
==================

* First Release
