package org.apache.spark.sql.execution.datasources.geoparquet.internal

import org.apache.spark.sql.internal.SQLConf

import java.util.{Locale, TimeZone}

/**
 * A more lenient SQLConf that works for both Apache Spark and Databricks Runtime. The whole point
 * is to avoid relying on the constants and methods defined in org.apache.spark.sql.internal.SQLConf,
 * since they are subject to change.
 */
object LenientSQLConf {
  val SESSION_LOCAL_TIMEZONE = buildConf("spark.sql.session.timeZone")
    .stringConf
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

  private def buildConf(key: String) = new ConfigBuilder(key)

  def get: LenientSQLConf = new LenientSQLConf(SQLConf.get)
}

class LenientSQLConf(sqlConf: SQLConf) {
  def sessionLocalTimeZone: String = getConf(LenientSQLConf.SESSION_LOCAL_TIMEZONE)

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
        conf.alternatives.iterator.map(sqlConf.getConfString(_, null)).find(_ != null)
          .map(conf.valueConverter)
          .getOrElse(conf.defaultValue.getOrElse(conf.defaultFunction.map(_()).get)))
    }
  }
}

class ConfigBuilder(val key: String, val alternatives: Seq[String] = Seq.empty) {
  def withAlternative(alternative: String): ConfigBuilder = {
    // This method is a placeholder for the alternative configuration handling.
    new ConfigBuilder(key, alternatives :+ alternative)
  }

  def stringConf = new TypedConfigBuilder[String](key, alternatives, (s: String) => s)
}

class TypedConfigBuilder[T](
  val key: String,
  val alternatives: Seq[String] = Seq.empty,
  val valueConverter: String => T,
  val transformer: T => T = (v: T) => v,
  val defaultValue: Option[T] = None,
  val defaultFunction: Option[() => T] = None
) {
  def transform(f: T => T): TypedConfigBuilder[T] = {
    // This method is a placeholder for the transformation logic.
    new TypedConfigBuilder[T](key, alternatives, valueConverter, f, defaultValue)
  }

  def createWithDefault(defaultValue: T): TypedConfig[T] = {
    // This method is a placeholder for creating the configuration with a default value.
    new TypedConfig[T](key, alternatives, valueConverter, transformer, Some(defaultValue))
  }

  def createWithDefaultFunction(f: () => T): TypedConfig[T] = {
    // This method is a placeholder for creating the configuration with a default function.
    new TypedConfig[T](key, alternatives, valueConverter, transformer, defaultValue, defaultFunction)
  }
}

case class TypedConfig[T](
  key: String,
  alternatives: Seq[String] = Seq.empty,
  valueConverter: String => T,
  transformer: T => T = (v: T) => v,
  defaultValue: Option[T] = None,
  defaultFunction: Option[() => T] = None)
