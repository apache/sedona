package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShpParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser;

import java.io.IOException;

/**
 * Created by zongsizhang on 5/3/17.
 */
public class ShapeFileReader extends RecordReader<ShapeKey, BytesWritable> {

    /** record id */
    private ShapeKey recordKey = null;

    /** primitive bytes value */
    private BytesWritable recordContent = null;

    /** inputstream for .shp file */
    private FSDataInputStream shpInputStream = null;

    /** file parser */
    ShpFileParser parser = null;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        ShpParseUtil.initializeGeometryFactory();
        FileSplit fileSplit = (FileSplit)split;
        Path filePath = fileSplit.getPath();
        FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
        shpInputStream = fileSys.open(filePath);
        //assign inputstream to parser and parse file header to init;
        parser = new ShpFileParser(shpInputStream);
        parser.parseShapeFileHead();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(ShpParseUtil.remainLength <= 0) return false;
        recordKey = new ShapeKey();
        recordContent = new BytesWritable();
        recordKey.setIndex(parser.parseRecordHeadID());
        byte[] primitiveContent = parser.parseRecordPrimitiveContent();
        recordContent.set(primitiveContent, 0, primitiveContent.length);
        return true;
    }

    public ShapeKey getCurrentKey() throws IOException, InterruptedException {
        return recordKey;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return recordContent;
    }

    public float getProgress() throws IOException, InterruptedException {
        return parser.getProgress();
    }

    public void close() throws IOException {
        shpInputStream.close();
    }
}
