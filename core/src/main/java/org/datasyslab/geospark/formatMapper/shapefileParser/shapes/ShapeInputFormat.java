package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;

/**
 * Created by zongsizhang on 5/3/17.
 */
public class ShapeInputFormat extends CombineFileInputFormat<ShapeKey, PrimitiveShape> {
    public RecordReader<ShapeKey, PrimitiveShape> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineShapeReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}

