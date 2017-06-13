package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zongsizhang on 5/3/17.
 */
public class ShapeKey implements Writable{

    LongWritable index = new LongWritable();

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(index.get());

    }

    public long getIndex() {
        return index.get();
    }

    public void setIndex(long _index) {
        index.set(_index);
    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
