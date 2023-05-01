/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.SPARK_VERSION_METADATA_KEY
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.util.Utils

import scala.util.Try

// Needed by Sedona to support Spark 3.0 - 3.3
object GeoDataSourceUtils {

  val PARQUET_REBASE_MODE_IN_READ = firstAvailableConf(
    "spark.sql.parquet.datetimeRebaseModeInRead",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead")
  val PARQUET_REBASE_MODE_IN_WRITE = firstAvailableConf(
    "spark.sql.parquet.datetimeRebaseModeInWrite",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite")
  val PARQUET_INT96_REBASE_MODE_IN_READ = firstAvailableConf(
    "spark.sql.parquet.int96RebaseModeInRead",
    "spark.sql.legacy.parquet.int96RebaseModeInRead",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead")
  val PARQUET_INT96_REBASE_MODE_IN_WRITE = firstAvailableConf(
    "spark.sql.parquet.int96RebaseModeInWrite",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite")

  private def firstAvailableConf(confs: String *): String = {
    confs.find(c => Try(SQLConf.get.getConfString(c)).isSuccess).get
  }

  def datetimeRebaseMode(
                          lookupFileMeta: String => String,
                          modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.0.0" || lookupFileMeta("org.apache.spark.legacyDateTime") != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }

  def int96RebaseMode(
                       lookupFileMeta: String => String,
                       modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 3.0 and earlier follow the legacy hybrid calendar and we need to
      // rebase the INT96 timestamp values.
      // Files written by Spark 3.1 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.1.0" || lookupFileMeta("org.apache.spark.legacyINT96") != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }

  def creteDateRebaseFuncInRead(
                                 rebaseMode: LegacyBehaviorPolicy.Value,
                                 format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchJulianDay) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def creteDateRebaseFuncInWrite(
    rebaseMode: LegacyBehaviorPolicy.Value,
    format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchGregorianDay) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def creteTimestampRebaseFuncInRead(
                                      rebaseMode: LegacyBehaviorPolicy.Value,
                                      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchJulianTs) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianMicros
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }

  def creteTimestampRebaseFuncInWrite(
    rebaseMode: LegacyBehaviorPolicy.Value,
    format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchGregorianTs) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianMicros
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }
}
