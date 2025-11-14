/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.execution.datasources.geoparquet.internal

import org.apache.spark.sql.internal.SQLConf

import java.util.{Locale, TimeZone}

/**
 * A more portable SQLConf that works for both Apache Spark and Databricks Runtime. The whole
 * point is to avoid relying on the constants and methods defined in
 * org.apache.spark.sql.internal.SQLConf, since they are subject to change. We only depend on the
 * getConfString method of SQLConf, which is more stable.
 */
object PortableSQLConf {

  // Enum for Parquet output timestamp types
  object ParquetOutputTimestampType extends Enumeration {
    val INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS = Value
  }
  val SESSION_LOCAL_TIMEZONE = buildConf("spark.sql.session.timeZone").stringConf
    .createWithDefaultFunction(() => TimeZone.getDefault.getID)

  val PARQUET_INT96_REBASE_MODE_IN_READ =
    buildConf("spark.sql.parquet.int96RebaseModeInRead")
      .withAlternative("spark.sql.legacy.parquet.int96RebaseModeInRead")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  val PARQUET_REBASE_MODE_IN_READ =
    buildConf("spark.sql.parquet.datetimeRebaseModeInRead")
      .withAlternative("spark.sql.legacy.parquet.datetimeRebaseModeInRead")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  val AVRO_REBASE_MODE_IN_READ =
    buildConf("spark.sql.avro.datetimeRebaseModeInRead")
      .withAlternative("spark.sql.legacy.avro.datetimeRebaseModeInRead")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  val PARQUET_INT96_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.parquet.int96RebaseModeInWrite")
      .withAlternative("spark.sql.legacy.parquet.int96RebaseModeInWrite")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  val PARQUET_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.parquet.datetimeRebaseModeInWrite")
      .withAlternative("spark.sql.legacy.parquet.datetimeRebaseModeInWrite")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  val AVRO_REBASE_MODE_IN_WRITE =
    buildConf("spark.sql.avro.datetimeRebaseModeInWrite")
      .withAlternative("spark.sql.legacy.avro.datetimeRebaseModeInWrite")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(LegacyBehaviorPolicy.EXCEPTION.toString)

  // Additional Parquet configurations
  val PARQUET_BINARY_AS_STRING = buildConf("spark.sql.parquet.binaryAsString").booleanConf
    .createWithDefault(false)

  val PARQUET_INT96_AS_TIMESTAMP = buildConf("spark.sql.parquet.int96AsTimestamp").booleanConf
    .createWithDefault(true)

  val CASE_SENSITIVE = buildConf("spark.sql.caseSensitive").booleanConf
    .createWithDefault(false)

  val PARQUET_INFER_TIMESTAMP_NTZ_ENABLED =
    buildConf("spark.sql.parquet.inferTimestampNTZ.enabled").booleanConf
      .createWithDefault(true)

  val LEGACY_PARQUET_NANOS_AS_LONG = buildConf("spark.sql.legacy.parquet.nanosAsLong").booleanConf
    .createWithDefault(false)

  val PARQUET_WRITE_LEGACY_FORMAT = buildConf("spark.sql.parquet.writeLegacyFormat").booleanConf
    .createWithDefault(false)

  val PARQUET_OUTPUT_TIMESTAMP_TYPE =
    buildConf("spark.sql.parquet.outputTimestampType").stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(ParquetOutputTimestampType.INT96.toString)

  val PARQUET_FIELD_ID_WRITE_ENABLED =
    buildConf("spark.sql.parquet.fieldId.write.enabled").booleanConf
      .createWithDefault(true)

  val PARQUET_FIELD_ID_READ_ENABLED =
    buildConf("spark.sql.parquet.fieldId.read.enabled").booleanConf
      .createWithDefault(false)

  val NESTED_SCHEMA_PRUNING_ENABLED =
    buildConf("spark.sql.optimizer.nestedSchemaPruning.enabled").booleanConf
      .createWithDefault(true)

  val IGNORE_MISSING_PARQUET_FIELD_ID =
    buildConf("spark.sql.parquet.fieldId.read.ignoreMissing").booleanConf
      .createWithDefault(false)

  val OUTPUT_COMMITTER_CLASS = buildConf(
    "spark.sql.sources.outputCommitterClass").stringConf.createOptional

  val PARQUET_OUTPUT_COMMITTER_CLASS = buildConf(
    "spark.sql.parquet.output.committer.class").stringConf.createOptional

  val PARQUET_AGGREGATE_PUSHDOWN_ENABLED =
    buildConf("spark.sql.parquet.aggregatePushdown").booleanConf
      .createWithDefault(true)

  val PARQUET_COMPRESSION = buildConf("spark.sql.parquet.compression.codec").stringConf
    .transform(_.toLowerCase(java.util.Locale.ROOT))
    .createWithDefault("snappy")

  val PARQUET_SCHEMA_MERGING_ENABLED = buildConf("spark.sql.parquet.mergeSchema").booleanConf
    .createWithDefault(false)

  val PARQUET_VECTORIZED_READER_ENABLED =
    buildConf("spark.sql.parquet.enableVectorizedReader").booleanConf
      .createWithDefault(true)

  val PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED =
    buildConf("spark.sql.parquet.enableNestedColumnVectorizedReader").booleanConf
      .createWithDefault(false)

  val PARQUET_RECORD_FILTER_ENABLED =
    buildConf("spark.sql.parquet.recordLevelFilter.enabled").booleanConf
      .createWithDefault(false)

  val PARQUET_INT96_TIMESTAMP_CONVERSION =
    buildConf("spark.sql.parquet.int96TimestampConversion").booleanConf
      .createWithDefault(false)

  val PARQUET_FILTER_PUSHDOWN_ENABLED = buildConf("spark.sql.parquet.filterPushdown").booleanConf
    .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DATE_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.date").booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.timestamp").booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.decimal").booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED =
    buildConf("spark.sql.parquet.filterPushdown.stringPredicate").booleanConf
      .createWithDefault(true)

  val PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD =
    buildConf("spark.sql.parquet.pushdown.inFilterThreshold").intConf
      .createWithDefault(10)

  val PARQUET_SCHEMA_RESPECT_SUMMARIES =
    buildConf("spark.sql.parquet.respectSummaryFiles").booleanConf
      .createWithDefault(false)

  val PARQUET_VECTORIZED_READER_BATCH_SIZE =
    buildConf("spark.sql.parquet.columnarReaderBatchSize").intConf
      .createWithDefault(4096)

  val IGNORE_CORRUPT_FILES = buildConf("spark.sql.files.ignoreCorruptFiles").booleanConf
    .createWithDefault(false)

  private def buildConf(key: String) = new ConfigBuilder(key)

  def get: PortableSQLConf = new PortableSQLConf(SQLConf.get)
}

class PortableSQLConf(sqlConf: SQLConf) {
  def sessionLocalTimeZone: String = getConf(PortableSQLConf.SESSION_LOCAL_TIMEZONE)

  def getConfString(key: String): String = {
    sqlConf.getConfString(key)
  }

  def getConfString(key: String, defaultValue: String): String = {
    sqlConf.getConfString(key, defaultValue)
  }

  def getConf[T](conf: TypedConfig[T]): T = {
    val value = sqlConf.getConfString(conf.key, null)
    if (value != null) {
      conf.transformer(conf.valueConverter(value))
    } else {
      conf.transformer(
        conf.alternatives.iterator
          .map(sqlConf.getConfString(_, null))
          .find(_ != null)
          .map(conf.valueConverter)
          .getOrElse(conf.defaultValue.getOrElse(conf.defaultFunction.map(_()).get)))
    }
  }

  def parquetCompressionCodec: String = getConf(PortableSQLConf.PARQUET_COMPRESSION)
  def isParquetSchemaMergingEnabled: Boolean = getConf(
    PortableSQLConf.PARQUET_SCHEMA_MERGING_ENABLED)
  def offHeapColumnVectorEnabled: Boolean =
    getConfString("spark.sql.columnVector.offheap.enabled", "false").toBoolean
  def isParquetBinaryAsString: Boolean = getConf(PortableSQLConf.PARQUET_BINARY_AS_STRING)
  def isParquetINT96AsTimestamp: Boolean = getConf(PortableSQLConf.PARQUET_INT96_AS_TIMESTAMP)
  def parquetInferTimestampNTZEnabled: Boolean = getConf(
    PortableSQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED)
  def writeLegacyParquetFormat: Boolean = getConf(PortableSQLConf.PARQUET_WRITE_LEGACY_FORMAT)
  def parquetOutputTimestampType: PortableSQLConf.ParquetOutputTimestampType.Value = {
    PortableSQLConf.ParquetOutputTimestampType.withName(
      getConf(PortableSQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE))
  }
  def caseSensitiveAnalysis: Boolean = getConf(PortableSQLConf.CASE_SENSITIVE)
  def parquetFieldIdReadEnabled: Boolean = getConf(PortableSQLConf.PARQUET_FIELD_ID_READ_ENABLED)
  def legacyParquetNanosAsLong: Boolean = getConf(PortableSQLConf.LEGACY_PARQUET_NANOS_AS_LONG)
  def parquetVectorizedReaderEnabled: Boolean = getConf(
    PortableSQLConf.PARQUET_VECTORIZED_READER_ENABLED)
  def parquetVectorizedReaderNestedColumnEnabled: Boolean = getConf(
    PortableSQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED)
  def parquetFieldIdWriteEnabled: Boolean = getConf(
    PortableSQLConf.PARQUET_FIELD_ID_WRITE_ENABLED)
  def nestedSchemaPruningEnabled: Boolean = getConf(PortableSQLConf.NESTED_SCHEMA_PRUNING_ENABLED)
  def parquetRecordFilterEnabled: Boolean = getConf(PortableSQLConf.PARQUET_RECORD_FILTER_ENABLED)
  def isParquetINT96TimestampConversion: Boolean = getConf(
    PortableSQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION)
  def parquetFilterPushDown: Boolean = getConf(PortableSQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)
  def parquetFilterPushDownDate: Boolean = getConf(
    PortableSQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED)
  def parquetFilterPushDownTimestamp: Boolean = getConf(
    PortableSQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED)
  def parquetFilterPushDownDecimal: Boolean = getConf(
    PortableSQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED)
  def parquetFilterPushDownStringPredicate: Boolean = getConf(
    PortableSQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED)
  def parquetFilterPushDownInFilterThreshold: Int = getConf(
    PortableSQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD)
  def isParquetSchemaRespectSummaries: Boolean = getConf(
    PortableSQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES)
  def parquetVectorizedReaderBatchSize: Int = getConf(
    PortableSQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE)

  def ignoreCorruptFiles: Boolean = getConf(PortableSQLConf.IGNORE_CORRUPT_FILES)
}

class ConfigBuilder(val key: String, val alternatives: Seq[String] = Seq.empty) {
  def withAlternative(alternative: String): ConfigBuilder = {
    new ConfigBuilder(key, alternatives :+ alternative)
  }

  def stringConf = new TypedConfigBuilder[String](key, alternatives, (s: String) => s)

  def booleanConf = new TypedConfigBuilder[Boolean](
    key,
    alternatives,
    (s: String) => s.toLowerCase(Locale.ROOT).toBoolean)

  def intConf = new TypedConfigBuilder[Int](key, alternatives, (s: String) => s.toInt)
}

class TypedConfigBuilder[T](
    val key: String,
    val alternatives: Seq[String] = Seq.empty,
    val valueConverter: String => T,
    val transformer: T => T = (v: T) => v) {
  def transform(f: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder[T](key, alternatives, valueConverter, f)
  }

  def createWithDefault(defaultValue: T): TypedConfig[T] = {
    new TypedConfig[T](key, alternatives, valueConverter, transformer, Some(defaultValue))
  }

  def createWithDefaultFunction(f: () => T): TypedConfig[T] = {
    new TypedConfig[T](key, alternatives, valueConverter, transformer, None, Some(f))
  }

  def createOptional: TypedConfig[Option[T]] = {
    // This method creates a configuration that returns None when not set
    new TypedConfig[Option[T]](
      key,
      alternatives,
      (s: String) => Some(valueConverter(s)),
      (v: Option[T]) => v,
      Some(None))
  }
}

case class TypedConfig[T](
    key: String,
    alternatives: Seq[String] = Seq.empty,
    valueConverter: String => T,
    transformer: T => T = (v: T) => v,
    defaultValue: Option[T] = None,
    defaultFunction: Option[() => T] = None)
