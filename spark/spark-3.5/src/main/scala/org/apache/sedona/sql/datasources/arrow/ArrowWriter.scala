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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class ArrowWriter() extends DataWriter[InternalRow] {

  private var encoder: AgnosticEncoder[Row] = _
  private var rowDeserializer: ExpressionEncoder.Deserializer[Row] = _
  private var serializer: ArrowSerializer[Row] = _
  private var dummyOutput: ByteArrayOutputStream = _
  private var rowCount: Long = 0

  def this(
      logicalInfo: LogicalWriteInfo,
      physicalInfo: PhysicalWriteInfo,
      partitionId: Int,
      taskId: Long) {
    this()
    dummyOutput = new ByteArrayOutputStream()
    encoder = RowEncoder.encoderFor(logicalInfo.schema())
    rowDeserializer = Encoders
      .row(logicalInfo.schema())
      .asInstanceOf[ExpressionEncoder[Row]]
      .resolveAndBind()
      .createDeserializer()
    serializer = new ArrowSerializer[Row](encoder, new RootAllocator(), "UTC");

    serializer.writeSchema(dummyOutput)
  }

  def write(record: InternalRow): Unit = {
    serializer.append(rowDeserializer.apply(record))
    rowCount = rowCount + 1
    if (shouldFlush()) {
      flush()
    }
  }

  private def shouldFlush(): Boolean = {
    // Can use serializer.sizeInBytes() to parameterize batch size in terms of bytes
    // or just use rowCount (batches of ~1024 rows are common and 16 MB is also a common
    // threshold (maybe also applying a minimum row count in case we're dealing with big
    // geometries). Checking sizeInBytes() should be done sparingly (expensive)).
    rowCount >= 1024
  }

  private def flush(): Unit = {
    serializer.writeBatch(dummyOutput)
  }

  def commit(): WriterCommitMessage = {
    null
  }

  def abort(): Unit = {}

  def close(): Unit = {
    flush()
    serializer.writeEndOfStream(dummyOutput)
    serializer.close()
    dummyOutput.close()
  }

}
