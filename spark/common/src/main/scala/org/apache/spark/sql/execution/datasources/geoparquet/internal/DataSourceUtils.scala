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

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.util.Utils

object DataSourceUtils extends PredicateHelper {

  /**
   * Metadata key which is used to write Spark version in the following:
   *   - Parquet file metadata
   *   - ORC file metadata
   *   - Avro file metadata
   *
   * Note that Hive table property `spark.sql.create.version` also has Spark version.
   */
  private[sql] val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"

  /**
   * The metadata key which is used to write the current session time zone into:
   *   - Parquet file metadata
   *   - Avro file metadata
   */
  private[sql] val SPARK_TIMEZONE_METADATA_KEY = "org.apache.spark.timeZone"

  /**
   * Parquet/Avro file metadata key to indicate that the file was written with legacy datetime
   * values.
   */
  private[sql] val SPARK_LEGACY_DATETIME_METADATA_KEY = "org.apache.spark.legacyDateTime"

  /**
   * Parquet file metadata key to indicate that the file with INT96 column type was written with
   * rebasing.
   */
  private[sql] val SPARK_LEGACY_INT96_METADATA_KEY = "org.apache.spark.legacyINT96"

  private def getRebaseSpec(
      lookupFileMeta: String => String,
      modeByConfig: String,
      minVersion: String,
      metadataKey: String): RebaseSpec = {
    val policy =
      if (Utils.isTesting &&
        PortableSQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
        LegacyBehaviorPolicy.CORRECTED
      } else {
        // If there is no version, we return the mode specified by the config.
        Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY))
          .map { version =>
            // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
            // rebase the datetime values.
            // Files written by `minVersion` and latter may also need the rebase if they were written
            // with the "LEGACY" rebase mode.
            if (version < minVersion || lookupFileMeta(metadataKey) != null) {
              LegacyBehaviorPolicy.LEGACY
            } else {
              LegacyBehaviorPolicy.CORRECTED
            }
          }
          .getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
      }
    policy match {
      case LegacyBehaviorPolicy.LEGACY =>
        RebaseSpec(
          LegacyBehaviorPolicy.LEGACY,
          Option(lookupFileMeta(SPARK_TIMEZONE_METADATA_KEY)))
      case _ => RebaseSpec(policy)
    }
  }

  def datetimeRebaseSpec(lookupFileMeta: String => String, modeByConfig: String): RebaseSpec = {
    getRebaseSpec(lookupFileMeta, modeByConfig, "3.0.0", SPARK_LEGACY_DATETIME_METADATA_KEY)
  }

  def int96RebaseSpec(lookupFileMeta: String => String, modeByConfig: String): RebaseSpec = {
    getRebaseSpec(lookupFileMeta, modeByConfig, "3.1.0", SPARK_LEGACY_INT96_METADATA_KEY)
  }

  def newRebaseExceptionInRead(format: String): RuntimeException = {
    val (config, option) = format match {
      case "Parquet INT96" =>
        (PortableSQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key, ParquetOptions.INT96_REBASE_MODE)
      case "Parquet" =>
        (PortableSQLConf.PARQUET_REBASE_MODE_IN_READ.key, ParquetOptions.DATETIME_REBASE_MODE)
      case "Avro" =>
        (PortableSQLConf.AVRO_REBASE_MODE_IN_READ.key, "datetimeRebaseMode")
      case _ => throw new IllegalStateException(s"Unrecognized format $format.")
    }
    QueryExecutionErrors.sparkUpgradeInReadingDatesError(format, config, option)
  }

  def newRebaseExceptionInWrite(format: String): RuntimeException = {
    val config = format match {
      case "Parquet INT96" => PortableSQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key
      case "Parquet" => PortableSQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
      case "Avro" => PortableSQLConf.AVRO_REBASE_MODE_IN_WRITE.key
      case _ => throw new IllegalStateException(s"Unrecognized format $format.")
    }
    QueryExecutionErrors.sparkUpgradeInWritingDatesError(format, config)
  }

  def createDateRebaseFuncInRead(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION =>
      days: Int =>
        if (days < RebaseDateTime.lastSwitchJulianDay) {
          throw DataSourceUtils.newRebaseExceptionInRead(format)
        }
        days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createDateRebaseFuncInWrite(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION =>
      days: Int =>
        if (days < RebaseDateTime.lastSwitchGregorianDay) {
          throw DataSourceUtils.newRebaseExceptionInWrite(format)
        }
        days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createTimestampRebaseFuncInRead(rebaseSpec: RebaseSpec, format: String): Long => Long =
    rebaseSpec.mode match {
      case LegacyBehaviorPolicy.EXCEPTION =>
        micros: Long =>
          if (micros < RebaseDateTime.lastSwitchJulianTs) {
            throw DataSourceUtils.newRebaseExceptionInRead(format)
          }
          micros
      case LegacyBehaviorPolicy.LEGACY =>
        RebaseDateTime.rebaseJulianToGregorianMicros(rebaseSpec.timeZone, _)
      case LegacyBehaviorPolicy.CORRECTED => identity[Long]
    }

  def createTimestampRebaseFuncInWrite(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION =>
      micros: Long =>
        if (micros < RebaseDateTime.lastSwitchGregorianTs) {
          throw DataSourceUtils.newRebaseExceptionInWrite(format)
        }
        micros
    case LegacyBehaviorPolicy.LEGACY =>
      val timeZone = PortableSQLConf.get.sessionLocalTimeZone
      RebaseDateTime.rebaseGregorianToJulianMicros(timeZone, _)
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }
}
