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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class CombineShapeReader
        extends RecordReader<ShapeKey, PrimitiveShape>
{

    /**
     * dubug logger
     */
    final static Logger logger = Logger.getLogger(CombineShapeReader.class);
    /**
     * suffix of attribute file
     */
    private final static String DBF_SUFFIX = "dbf";
    /**
     * suffix of shape record file
     */
    private final static String SHP_SUFFIX = "shp";
    /**
     * suffix of index file
     */
    private final static String SHX_SUFFIX = "shx";
    /**
     * id of input path of .shp file
     */
    private FileSplit shpSplit = null;
    /**
     * id of input path of .shx file
     */
    private FileSplit shxSplit = null;
    /**
     * id of input path of .dbf file
     */
    private FileSplit dbfSplit = null;
    /**
     * RecordReader for .shp file
     */
    private ShapeFileReader shapeFileReader = null;
    /**
     * RecordReader for .dbf file
     */
    private DbfFileReader dbfFileReader = null;
    /**
     * flag of whether .dbf exists
     */
    private boolean hasDbf = false;
    /**
     * flag of whether having next .dbf record
     */
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
            throws IOException, InterruptedException
    {
        CombineFileSplit fileSplit = (CombineFileSplit) split;
        Path[] paths = fileSplit.getPaths();
        for (int i = 0; i < paths.length; ++i) {
            String suffix = FilenameUtils.getExtension(paths[i].toString()).toLowerCase();
            if (suffix.equals(SHP_SUFFIX)) { shpSplit = new FileSplit(paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations()); }
            else if (suffix.equals(SHX_SUFFIX)) { shxSplit = new FileSplit(paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations()); }
            else if (suffix.equals(DBF_SUFFIX)) { dbfSplit = new FileSplit(paths[i], fileSplit.getOffset(i), fileSplit.getLength(i), fileSplit.getLocations()); }
        }
        // if shape file doesn't exists, throw an IOException
        if (shpSplit == null) { throw new IOException("Can't find .shp file."); }
        else {
            if (shxSplit != null) {
                // shape file exists, extract .shp with .shx
                // first read all indexes into memory
                Path filePath = shxSplit.getPath();
                FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
                FSDataInputStream shxInpuStream = fileSys.open(filePath);
                shxInpuStream.skip(24);
                int shxFileLength = shxInpuStream.readInt() * 2 - 100; // get length in bytes, exclude header
                // skip following 72 bytes in header
                shxInpuStream.skip(72);
                byte[] bytes = new byte[shxFileLength];
                // read all indexes into memory, skip first 50 bytes(header)
                shxInpuStream.readFully(bytes, 0, bytes.length);
                IntBuffer buffer = ByteBuffer.wrap(bytes).asIntBuffer();
                int[] indexes = new int[shxFileLength / 4];
                buffer.get(indexes);
                shapeFileReader = new ShapeFileReader(indexes);
            }
            else {
                shapeFileReader = new ShapeFileReader(); // no index, construct with no parameter
            }
            shapeFileReader.initialize(shpSplit, context);
        }
        if (dbfSplit != null) {
            dbfFileReader = new DbfFileReader();
            dbfFileReader.initialize(dbfSplit, context);
            hasDbf = true;
        }
        else { hasDbf = false; }
    }

    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {

        boolean hasNextShp = shapeFileReader.nextKeyValue();
        if (hasDbf) { hasNextDbf = dbfFileReader.nextKeyValue(); }

        int curShapeType = shapeFileReader.getCurrentValue().getTypeID();
        while (hasNextShp && ShapeType.getType(curShapeType) == ShapeType.UNDEFINED) {
            if (hasDbf) { hasNextDbf = dbfFileReader.nextKeyValue(); }
            hasNextShp = shapeFileReader.nextKeyValue();
            curShapeType = shapeFileReader.getCurrentValue().getTypeID();
        }
        // check if records match in .shp and .dbf
        if (hasDbf) {
            if (hasNextShp && !hasNextDbf) {
                Exception e = new Exception("shape record loses attributes in .dbf file at ID=" + shapeFileReader.getCurrentKey().getIndex());
                e.printStackTrace();
            }
            else if (!hasNextShp && hasNextDbf) {
                Exception e = new Exception("Redundant attributes in .dbf exists");
                e.printStackTrace();
            }
        }
        return hasNextShp;
    }

    public ShapeKey getCurrentKey()
            throws IOException, InterruptedException
    {
        return shapeFileReader.getCurrentKey();
    }

    public PrimitiveShape getCurrentValue()
            throws IOException, InterruptedException
    {
        PrimitiveShape value = new PrimitiveShape(shapeFileReader.getCurrentValue());
        if (hasDbf && hasNextDbf) { value.setAttributes(dbfFileReader.getCurrentValue()); }
        return value;
    }

    public float getProgress()
            throws IOException, InterruptedException
    {
        return shapeFileReader.getProgress();
    }

    public void close()
            throws IOException
    {
        shapeFileReader.close();
    }
}
