/**
 * FILE: ShapeInputFormat.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeInputFormat.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;

public class ShapeInputFormat extends CombineFileInputFormat<ShapeKey, PrimitiveShape> {
    public RecordReader<ShapeKey, PrimitiveShape> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineShapeReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}

