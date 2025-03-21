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
package org.apache.sedona.sql.datasources.arrow;

import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

class ArrowTable implements SupportsWrite, SupportsOverwrite {
  private Set<TableCapability> capabilities;
  private StructType schema;

  ArrowTable(StructType schema) {
    this.schema = schema;
  }

  @Override
  public String name() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'name'");
  }

  @Override
  public StructType schema() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'schema'");
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      this.capabilities = new HashSet<>();
      capabilities.add(TableCapability.BATCH_WRITE);
    }
    return capabilities;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new ArrowWriteBuilder(info);
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'overwrite'");
  }
}
