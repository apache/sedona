package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;

import java.io.IOException;

/**
 * Created by zongsizhang on 6/2/17.
 */
public class DbfFileReader extends org.apache.hadoop.mapreduce.RecordReader<ShapeKey, BytesWritable> {

    /** inputstream of .dbf file */
    private FSDataInputStream inputStream = null;

    /** primitive bytes array of one row */
    private BytesWritable value = null;

    /** key value of current row */
    private ShapeKey key = null;

    /** generated id of current row */
    private int id = 0;

    /** Info Bundle of current dbf file */
    private DbfParseUtil.DbfInfoBundle dbfInfo = null;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        Path inputPath = fileSplit.getPath();
        FileSystem fileSys = inputPath.getFileSystem(context.getConfiguration());
        inputStream = fileSys.open(inputPath);
        dbfInfo = DbfParseUtil.parseFileHead(inputStream);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        // first check deleted flag
        byte[] curbytes = DbfParseUtil.parsePrimitiveRecord(inputStream, dbfInfo);
        if(curbytes == null){
            value = null;
            return false;
        }
        else{
            value = new BytesWritable(curbytes);
            key = new ShapeKey();
            key.setIndex(id++);
            return true;
        }
    }

    public ShapeKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return dbfInfo.getProgress();
    }

    public void close() throws IOException {

    }
}
