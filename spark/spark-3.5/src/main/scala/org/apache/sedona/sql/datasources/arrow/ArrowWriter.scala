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
package org.apache.sedona.sql.datasources.arrow

import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.Row;
import java.io.ByteArrayOutputStream
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

case class ArrowWriter() extends DataWriter[InternalRow] {

  private var encoder: AgnosticEncoder[Row] = _
  private var serializer: ArrowSerializer[Row] = _
  private var dummyOutput: ByteArrayOutputStream = _

  def this(
      logicalInfo: LogicalWriteInfo,
      physicalInfo: PhysicalWriteInfo,
      partitionId: Int,
      taskId: Long) {
    this()
    dummyOutput = new ByteArrayOutputStream()
    encoder = RowEncoder.encoderFor(logicalInfo.schema())
    serializer = new ArrowSerializer[Row](encoder, new RootAllocator(), "UTC");
  }

  def write(record: InternalRow): Unit = {}

  def commit(): WriterCommitMessage = {
    null
  }

  def abort(): Unit = {}

  def close(): Unit = {}

}
