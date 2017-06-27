/**
 * FILE: ShapeFileReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeFileReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser;

import java.io.IOException;

public class ShapeFileReader extends RecordReader<ShapeKey, ShpRecord> {

    /** record id */
    private ShapeKey recordKey = null;

    /** primitive bytes value */
    private ShpRecord recordContent = null;

    /** inputstream for .shp file */
    private FSDataInputStream shpInputStream = null;

    /** file parser */
    ShpFileParser parser = null;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        Path filePath = fileSplit.getPath();
        FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
        shpInputStream = fileSys.open(filePath);
        //assign inputstream to parser and parse file header to init;
        parser = new ShpFileParser(shpInputStream);
        parser.parseShapeFileHead();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(getProgress() >= 1) return false;
        recordKey = new ShapeKey();
        recordKey.setIndex(parser.parseRecordHeadID());
        recordContent = parser.parseRecordPrimitiveContent();
        return true;
    }

    public ShapeKey getCurrentKey() throws IOException, InterruptedException {
        return recordKey;
    }

    public ShpRecord getCurrentValue() throws IOException, InterruptedException {
        return recordContent;
    }

    public float getProgress() throws IOException, InterruptedException {
        return parser.getProgress();
    }

    public void close() throws IOException {
        shpInputStream.close();
    }
}
