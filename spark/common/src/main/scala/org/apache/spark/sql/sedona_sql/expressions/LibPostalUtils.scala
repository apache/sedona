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

import com.mapzen.jpostal.{AddressExpander, AddressParser, Config}

object LibPostalUtils {

  private def getConfig(dataDir: String, useSenzing: Boolean): Config = {
    // Resolve the data directory: if it points to a remote filesystem (HDFS, S3, etc.),
    // copy the data locally first since jpostal requires local filesystem access.
    val resolvedDir = LibPostalDataLoader.resolveDataDir(dataDir)
    val isRemote = LibPostalDataLoader.isRemotePath(dataDir)

    Config
      .builder()
      .dataDir(resolvedDir)
      // When data was fetched from a remote store, it is already fully populated;
      // disable jpostal's built-in internet download to avoid network access on the cluster.
      .downloadDataIfNeeded(!isRemote)
      .senzing(useSenzing)
      .build()
  }

  def getParserFromConf(dataDir: String, useSenzing: Boolean): AddressParser =
    AddressParser.getInstanceConfig(getConfig(dataDir, useSenzing))

  def getExpanderFromConf(dataDir: String, useSenzing: Boolean): AddressExpander =
    AddressExpander.getInstanceConfig(getConfig(dataDir, useSenzing))
}
