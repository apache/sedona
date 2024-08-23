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
package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ShxFileReader {

  public static int[] readAll(InputSplit split, TaskAttemptContext context) throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    Path inputPath = fileSplit.getPath();
    FileSystem fileSys = inputPath.getFileSystem(context.getConfiguration());
    try (FSDataInputStream stream = fileSys.open(inputPath)) {
      return readAll(stream);
    }
  }

  public static int[] readAll(DataInputStream stream) throws IOException {
    if (stream.skip(24) != 24) {
      throw new IOException("Failed to skip 24 bytes in .shx file");
    }
    int shxFileLength = stream.readInt() * 2 - 100; // get length in bytes, exclude header
    // skip following 72 bytes in header
    if (stream.skip(72) != 72) {
      throw new IOException("Failed to skip 72 bytes in .shx file");
    }
    byte[] bytes = new byte[shxFileLength];
    // read all indexes into memory, skip first 50 bytes(header)
    stream.readFully(bytes, 0, bytes.length);
    IntBuffer buffer = ByteBuffer.wrap(bytes).asIntBuffer();
    int[] indexes = new int[shxFileLength / 4];
    buffer.get(indexes);
    return indexes;
  }
}
