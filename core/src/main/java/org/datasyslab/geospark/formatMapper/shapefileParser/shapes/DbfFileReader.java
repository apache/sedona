/*
 * FILE: DbfFileReader
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;

import java.io.IOException;

public class DbfFileReader
        extends org.apache.hadoop.mapreduce.RecordReader<ShapeKey, String>
{

    /**
     * inputstream of .dbf file
     */
    private FSDataInputStream inputStream = null;

    /**
     * primitive bytes array of one row
     */
    private String value = null;

    /**
     * key value of current row
     */
    private ShapeKey key = null;

    /**
     * generated id of current row
     */
    private int id = 0;

    /**
     * Dbf parser
     */
    DbfParseUtil dbfParser = null;

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit) split;
        Path inputPath = fileSplit.getPath();
        FileSystem fileSys = inputPath.getFileSystem(context.getConfiguration());
        inputStream = fileSys.open(inputPath);
        dbfParser = new DbfParseUtil();
        dbfParser.parseFileHead(inputStream);
    }

    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
        // first check deleted flag
        String curbytes = dbfParser.parsePrimitiveRecord(inputStream);
        if (curbytes == null) {
            value = null;
            return false;
        }
        else {
            value = new String(curbytes);
            key = new ShapeKey();
            key.setIndex(id++);
            return true;
        }
    }

    public ShapeKey getCurrentKey()
            throws IOException, InterruptedException
    {
        return key;
    }

    public String getCurrentValue()
            throws IOException, InterruptedException
    {
        return value;
    }

    public float getProgress()
            throws IOException, InterruptedException
    {
        return dbfParser.getProgress();
    }

    public void close()
            throws IOException
    {
        inputStream.close();
    }
}
