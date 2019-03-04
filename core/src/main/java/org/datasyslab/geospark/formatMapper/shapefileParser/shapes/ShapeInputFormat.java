/*
 * FILE: ShapeInputFormat
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapeInputFormat
        extends CombineFileInputFormat<ShapeKey, PrimitiveShape>
{
    public RecordReader<ShapeKey, PrimitiveShape> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException
    {
        return new CombineShapeReader();
    }

    /**
     * enforce isSplitable() to return false so that every getSplits() only return one InputSplit
     *
     * @param context
     * @param file
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job)
            throws IOException
    {
        // get input paths and assign a split for every single path
        String path = job.getConfiguration().get("mapred.input.dir");
        String[] paths = path.split(",");
        List<InputSplit> splits = new ArrayList<>();
        for (int i = 0; i < paths.length; ++i) {
            job.getConfiguration().set("mapred.input.dir", paths[i]);
            splits.add(super.getSplits(job).get(0));
        }
        return splits;
    }
}

