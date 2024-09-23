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

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeType;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser;

public class ShapeFileReader extends RecordReader<ShapeKey, ShpRecord> {

  /** file parser */
  ShpFileParser parser = null;
  /** record id */
  private ShapeKey recordKey = null;
  /** primitive bytes value */
  private ShpRecord recordContent = null;
  /** input stream for .shp file */
  private FSDataInputStream shpInputStream = null;
  /** Iterator of indexes of records */
  private int[] indexes;

  /** whether to use index, true when using indexes */
  private boolean useIndex = false;

  /** current index id */
  private int indexId = 0;

  /** empty constructor */
  public ShapeFileReader() {}

  /**
   * constructor with index
   *
   * @param indexes offsets of records in the .shp file
   */
  public ShapeFileReader(int[] indexes) {
    this.indexes = indexes;
    useIndex = true;
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    Path filePath = fileSplit.getPath();
    FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
    FSDataInputStream stream = fileSys.open(filePath);
    initialize(stream);
  }

  public void initialize(FSDataInputStream stream) throws IOException {
    shpInputStream = stream;
    parser = new ShpFileParser(stream);
    parser.parseShapeFileHead();
  }

  public boolean nextKeyValue() throws IOException {
    if (useIndex) {
      /* with index, iterate until end and extract bytes with information from indexes */
      if (indexId == indexes.length) {
        return false;
      }
      // check offset, if current offset in inputStream not match with information in shx, move it
      long pos = indexes[indexId] * 2L;
      if (shpInputStream.getPos() < pos) {
        long skipBytes = pos - shpInputStream.getPos();
        if (shpInputStream.skip(skipBytes) != skipBytes) {
          throw new IOException("Failed to seek to the right place in .shp file");
        }
      }
      int currentLength = indexes[indexId + 1] * 2 - 4;
      recordKey = new ShapeKey();
      recordKey.setIndex(parser.parseRecordHeadID());
      if (currentLength >= 0) {
        recordContent = parser.parseRecordPrimitiveContent(currentLength);
      } else {
        // Ignore this index entry
        recordContent = new ShpRecord(new byte[0], ShapeType.NULL.getId());
      }
      indexId += 2;
    } else {
      if (getProgress() >= 1) {
        return false;
      }
      recordKey = new ShapeKey();
      recordKey.setIndex(parser.parseRecordHeadID());
      recordContent = parser.parseRecordPrimitiveContent();
    }
    return true;
  }

  public ShapeKey getCurrentKey() {
    return recordKey;
  }

  public ShpRecord getCurrentValue() {
    return recordContent;
  }

  public float getProgress() {
    return parser.getProgress();
  }

  public void close() throws IOException {
    shpInputStream.close();
  }
}
