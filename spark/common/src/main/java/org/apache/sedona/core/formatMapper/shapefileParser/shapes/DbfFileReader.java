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
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor;

public class DbfFileReader extends org.apache.hadoop.mapreduce.RecordReader<ShapeKey, String> {

  /** Dbf parser */
  DbfParseUtil dbfParser = null;
  /** inputstream of .dbf file */
  private FSDataInputStream inputStream = null;
  /** primitive bytes array of one row */
  private List<byte[]> value = null;
  /** key value of current row */
  private ShapeKey key = null;
  /** generated id of current row */
  private int id = 0;

  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    Path inputPath = fileSplit.getPath();
    FileSystem fileSys = inputPath.getFileSystem(context.getConfiguration());
    FSDataInputStream stream = fileSys.open(inputPath);
    initialize(stream);
  }

  public void initialize(FSDataInputStream stream) throws IOException {
    inputStream = stream;
    dbfParser = new DbfParseUtil();
    dbfParser.parseFileHead(inputStream);
  }

  public List<FieldDescriptor> getFieldDescriptors() {
    return dbfParser.getFieldDescriptors();
  }

  public boolean nextKeyValue() throws IOException {
    // first check deleted flag
    List<byte[]> fieldBytesList = dbfParser.parse(inputStream);
    if (fieldBytesList == null) {
      value = null;
      return false;
    } else {
      value = fieldBytesList;
      key = new ShapeKey();
      key.setIndex(id++);
      return true;
    }
  }

  public ShapeKey getCurrentKey() {
    return key;
  }

  public List<byte[]> getCurrentFieldBytes() {
    return value;
  }

  public String getCurrentValue() {
    if (value == null) {
      return null;
    }
    return DbfParseUtil.fieldBytesToString(value);
  }

  public float getProgress() {
    return dbfParser.getProgress();
  }

  public void close() throws IOException {
    inputStream.close();
  }
}
