/*
 * FILE: FieldnameRecordReader
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
package org.datasyslab.geospark.formatMapper.shapefileParser.fieldname;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor;

import java.io.IOException;
import java.util.List;

public class FieldnameRecordReader
        extends RecordReader<Long, String>
{

    /**
     * paths of files to be read
     */
    Path[] paths = null;

    /**
     * fixed key value for reduce all results together
     */
    long KEY_VALUE = 0;

    /**
     * input stream
     */
    FSDataInputStream inputStream = null;

    /**
     * task context
     */
    Configuration configuration = null;

    /**
     * index of current file to be read
     */
    int id = -1;

    /**
     * Dbf parser that is used to read field names
     */
    DbfParseUtil dbfParser = null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException
    {
        CombineFileSplit split = (CombineFileSplit) inputSplit;
        paths = split.getPaths();
        configuration = taskAttemptContext.getConfiguration();
        id = -1;
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
        id++;
        return id < paths.length;
    }

    @Override
    public Long getCurrentKey()
            throws IOException, InterruptedException
    {
        return KEY_VALUE;
    }

    @Override
    public String getCurrentValue()
            throws IOException, InterruptedException
    {
        // open id file
        FileSystem fs = paths[id].getFileSystem(configuration);
        inputStream = fs.open(paths[id]);
        // read header into memory
        dbfParser = new DbfParseUtil();
        dbfParser.parseFileHead(inputStream);
        String fieldNames = "";
        List<FieldDescriptor> fieldDescriptors = dbfParser.getFieldDescriptors();
        for (int i=0;i<fieldDescriptors.size();i++)
        {
            if (i==0)
            {
                fieldNames+=fieldDescriptors.get(i).getFieldName();
            }
            else {fieldNames+="\t"+fieldDescriptors.get(i).getFieldName();}
        }
        return fieldNames;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException
    {
        return (float) id / (float) paths.length;
    }

    @Override
    public void close()
            throws IOException
    {
        // input stream already closed every time getCurrentKey()
    }
}
