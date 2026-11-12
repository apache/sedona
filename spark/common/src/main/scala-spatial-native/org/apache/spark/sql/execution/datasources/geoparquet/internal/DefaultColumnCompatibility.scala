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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.{CatalogManager, DefaultCatalogManager, FunctionCatalog}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.internal.connector.V1Function

/** Keeps Spark's catalog API differences out of GeoParquet default-value analysis. */
private[internal] object DefaultColumnCompatibility {
  def catalogManager(
      functionCatalog: FunctionCatalog,
      sessionCatalog: SessionCatalog): CatalogManager = {
    new DefaultCatalogManager(functionCatalog, sessionCatalog)
  }

  def loadFunction(
      sessionCatalog: SessionCatalog,
      identifier: FunctionIdentifier): UnboundFunction = {
    V1Function(
      sessionCatalog.lookupPersistentFunction(identifier),
      (arguments: Seq[Expression]) => sessionCatalog.lookupFunction(identifier, arguments))
  }
}
