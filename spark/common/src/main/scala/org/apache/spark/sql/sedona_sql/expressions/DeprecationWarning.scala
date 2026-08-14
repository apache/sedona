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
package org.apache.spark.sql.sedona_sql.expressions

import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory

/**
 * Emits a single warning per JVM when a deprecated SQL function name is used, pointing the user
 * at the canonical name. A runtime warning is the only channel that reaches SQL users, who never
 * see Scala `@deprecated` annotations.
 */
private[expressions] object DeprecationWarning {
  private val LOGGER = LoggerFactory.getLogger(getClass)
  private val warned = ConcurrentHashMap.newKeySet[String]()

  def warnOnce(deprecatedName: String, canonicalName: String): Unit = {
    if (warned.add(deprecatedName)) {
      LOGGER.warn(
        s"The function $deprecatedName is deprecated and will be removed in a future release. " +
          s"Use $canonicalName instead.")
    }
  }
}
