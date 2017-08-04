/**
 * FILE: ShapeInputFormat.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeInputFormat.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapeInputFormat extends CombineFileInputFormat<ShapeKey, PrimitiveShape> {
    public RecordReader<ShapeKey, PrimitiveShape> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineShapeReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        String path = job.getConfiguration().get("mapred.input.dir");
        String[] paths = path.split(",");
        List<InputSplit> splits = new ArrayList<>();
        for(int i = 0;i < paths.length; ++i){
            job.getConfiguration().set("mapred.input.dir", paths[i]);
            splits.add(super.getSplits(job).get(0));
        }
        return splits;
    }
}

