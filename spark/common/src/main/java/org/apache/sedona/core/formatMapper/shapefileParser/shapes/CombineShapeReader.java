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
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombineShapeReader extends RecordReader<ShapeKey, PrimitiveShape> {

  /** debug logger */
  private static final Logger logger = LoggerFactory.getLogger(CombineShapeReader.class);
  /** suffix of attribute file */
  private static final String DBF_SUFFIX = "dbf";
  /** suffix of shape record file */
  private static final String SHP_SUFFIX = "shp";
  /** suffix of index file */
  private static final String SHX_SUFFIX = "shx";
  /** id of input path of .shp file */
  private FileSplit shpSplit = null;
  /** id of input path of .shx file */
  private FileSplit shxSplit = null;
  /** id of input path of .dbf file */
  private FileSplit dbfSplit = null;
  /** RecordReader for .shp file */
  private ShapeFileReader shapeFileReader = null;
  /** RecordReader for .dbf file */
  private DbfFileReader dbfFileReader = null;
  /** flag of whether .dbf exists */
  private boolean hasDbf = false;
  /** flag of whether having next .dbf record */
  private boolean hasNextDbf = false;

  /**
   * cut the combined split into FileSplit for .shp, .shx and .dbf
   *
   * @param split
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    CombineFileSplit fileSplit = (CombineFileSplit) split;
    Path[] paths = fileSplit.getPaths();
    for (int i = 0; i < paths.length; ++i) {
      String suffix = FilenameUtils.getExtension(paths[i].toString()).toLowerCase();
      if (suffix.equals(SHP_SUFFIX)) {
        shpSplit =
            new FileSplit(
                paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations());
      } else if (suffix.equals(SHX_SUFFIX)) {
        shxSplit =
            new FileSplit(
                paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations());
      } else if (suffix.equals(DBF_SUFFIX)) {
        dbfSplit =
            new FileSplit(
                paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations());
      }
    }
    // if shape file doesn't exist, throw an IOException
    if (shpSplit == null) {
      throw new IOException("Can't find .shp file.");
    } else {
      if (shxSplit != null) {
        // shape file exists, extract .shp with .shx
        // first read all indexes into memory
        int[] indexes = ShxFileReader.readAll(shxSplit, context);
        shapeFileReader = new ShapeFileReader(indexes);
      } else {
        shapeFileReader = new ShapeFileReader(); // no index, construct with no parameter
      }
      shapeFileReader.initialize(shpSplit, context);
    }
    if (dbfSplit != null) {
      dbfFileReader = new DbfFileReader();
      dbfFileReader.initialize(dbfSplit, context);
      hasDbf = true;
    } else {
      hasDbf = false;
    }
  }

  public boolean nextKeyValue() throws IOException {

    boolean hasNextShp = shapeFileReader.nextKeyValue();
    if (hasDbf) {
      hasNextDbf = dbfFileReader.nextKeyValue();
    }

    ShapeType curShapeType = shapeFileReader.getCurrentValue().getType();
    while (hasNextShp && !curShapeType.isSupported()) {
      logger.warn(
          "[SEDONA] Shapefile type {} is not supported. Skipped this record. Please use QGIS or GeoPandas to convert it to a type listed in ShapeType.java",
          curShapeType.name());
      if (hasDbf) {
        hasNextDbf = dbfFileReader.nextKeyValue();
      }
      hasNextShp = shapeFileReader.nextKeyValue();
      curShapeType = shapeFileReader.getCurrentValue().getType();
    }
    // check if records match in .shp and .dbf
    if (hasDbf) {
      if (hasNextShp && !hasNextDbf) {
        Exception e =
            new Exception(
                "shape record loses attributes in .dbf file at ID="
                    + shapeFileReader.getCurrentKey().getIndex());
        logger.warn(e.getMessage(), e);
      } else if (!hasNextShp && hasNextDbf) {
        Exception e = new Exception("Redundant attributes in .dbf exists");
        logger.warn(e.getMessage(), e);
      }
    }
    return hasNextShp;
  }

  public ShapeKey getCurrentKey() {
    return shapeFileReader.getCurrentKey();
  }

  public PrimitiveShape getCurrentValue() {
    PrimitiveShape value = new PrimitiveShape(shapeFileReader.getCurrentValue());
    if (hasDbf && hasNextDbf) {
      value.setAttributes(dbfFileReader.getCurrentValue());
    }
    return value;
  }

  public float getProgress() {
    return shapeFileReader.getProgress();
  }

  public void close() throws IOException {
    shapeFileReader.close();
  }
}
