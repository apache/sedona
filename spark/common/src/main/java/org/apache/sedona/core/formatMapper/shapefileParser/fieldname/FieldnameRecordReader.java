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

package org.apache.sedona.core.formatMapper.shapefileParser.fieldname;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor;

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
        for (int i = 0; i < fieldDescriptors.size(); i++) {
            if (i == 0) {
                fieldNames += fieldDescriptors.get(i).getFieldName();
            }
            else {fieldNames += "\t" + fieldDescriptors.get(i).getFieldName();}
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
