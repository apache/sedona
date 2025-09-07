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
package org.apache.sedona.sql.datasources.geopackage.transform

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

object DataTypesTransformations {
  def getDays(dateString: String): Int = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val date = LocalDate.parse(dateString, formatter)

    val epochDate = LocalDate.of(1970, 1, 1)

    ChronoUnit.DAYS.between(epochDate, date).toInt
  }

  def epoch(timestampStr: String): Long = {
    try {
      // Try parsing as-is first (works for timestamps with timezone info)
      Instant.parse(timestampStr).toEpochMilli
    } catch {
      case _: DateTimeParseException =>
        // If parsing fails, try treating it as UTC (common case for GeoPackage)
        try {
          // Handle various datetime formats without timezone info
          // Try different patterns to handle various millisecond formats
          val patterns = Array(
            "yyyy-MM-dd'T'HH:mm:ss.SSS", // 3 digits
            "yyyy-MM-dd'T'HH:mm:ss.SS", // 2 digits
            "yyyy-MM-dd'T'HH:mm:ss.S", // 1 digit
            "yyyy-MM-dd'T'HH:mm:ss" // no milliseconds
          )

          var localDateTime: LocalDateTime = null
          var lastException: DateTimeParseException = null

          for (pattern <- patterns) {
            try {
              val formatter = DateTimeFormatter.ofPattern(pattern)
              localDateTime = LocalDateTime.parse(timestampStr, formatter)
              lastException = null
            } catch {
              case e: DateTimeParseException =>
                lastException = e
            }
          }

          if (localDateTime != null) {
            localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli
          } else {
            throw lastException
          }
        } catch {
          case e: DateTimeParseException =>
            throw new IllegalArgumentException(
              s"Unable to parse datetime: $timestampStr. " +
                s"Expected formats: 'yyyy-MM-ddTHH:mm:ss[.S]' or 'yyyy-MM-ddTHH:mm:ss[.S]Z'",
              e)
        }
    }
  }
}
