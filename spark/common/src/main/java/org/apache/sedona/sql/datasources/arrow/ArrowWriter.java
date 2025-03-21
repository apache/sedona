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

import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

class ArrowWriter implements DataWriter<InternalRow> {
  private final int partitionId;
  private final long taskId;
  private int rowCount;
  private AgnosticEncoder<Row> encoder;
  private Encoder<Row> rowEncoder;
  // https://github.com/apache/spark/blob/9353e94e50f3f73565f5f0023effd7e265c177b9/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/client/arrow/ArrowSerializer.scala#L50
  private ArrowSerializer<Row> serializer;

  public ArrowWriter(int partitionId, long taskId, StructType schema) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.rowCount = 0;
    this.encoder = RowEncoder.encoderFor(schema);
    this.serializer = new ArrowSerializer<Row>(encoder, new RootAllocator(), "UTC");

    // Create file, write schema
    // Problem: ArrowSerializer() does not expose internal to write just the schema
    // bytes.
  }

  @Override
  public void close() throws IOException {
    // Close file
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'close'");
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // Problem: serializer needs a Row but we have an InternalRow
    // serializer.append(encoder.fromRow(record));

    rowCount++;
    if (rowCount > 1024) {
      // Problem: writeIpcStream() writes both the schema and the batch, but
      // we only want the batch
      // serializer.writeIpcStream(null);
      rowCount = 0;
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'commit'");
  }

  @Override
  public void abort() throws IOException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'abort'");
  }
}
