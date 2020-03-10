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
