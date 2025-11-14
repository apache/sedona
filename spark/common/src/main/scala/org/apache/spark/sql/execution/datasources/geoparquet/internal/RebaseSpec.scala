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

import java.util.TimeZone

object LegacyBehaviorPolicy extends Enumeration {
  val EXCEPTION, LEGACY, CORRECTED = Value
}

// Specification of rebase operation including `mode` and the time zone in which it is performed
case class RebaseSpec(mode: LegacyBehaviorPolicy.Value, originTimeZone: Option[String] = None) {
  // Use the default JVM time zone for backward compatibility
  def timeZone: String = originTimeZone.getOrElse(TimeZone.getDefault.getID)
}
