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

import com.google.common.primitives.Longs;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.*;

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
        String parentpath = job.getConfiguration().get("mapred.input.dir");
        String[] childpaths = parentpath.split(",");
        List<InputSplit> splits = new ArrayList<>();
        for (String childPath : childpaths) {
            job.getConfiguration().set("mapred.input.dir", childPath);
            // get all files in input file path and sort for filename order
            CombineFileSplit childPathFileSplits = (CombineFileSplit) super.getSplits(job).get(0);
            Path[] filePaths = childPathFileSplits.getPaths();
            long[] fileSizes = childPathFileSplits.getLengths();

            List<Path> fileSplitPathParts = new ArrayList<>();
            List<Long> fileSplitSizeParts = new ArrayList<>();
            String prevfilename = "";
            int j = 0;

            while (j < filePaths.length) {
                String filename = FilenameUtils.removeExtension(filePaths[j].toString()).toLowerCase();
                if (fileSplitPathParts.size() == 0 || prevfilename.equals(filename)) {
                    fileSplitPathParts.add(filePaths[j]);
                    fileSplitSizeParts.add(fileSizes[j]);
                }
                // compare file name and if it is different then all same filename is into CombileFileSplit
                else {
                    splits.add(new CombineFileSplit(fileSplitPathParts.toArray(new Path[0]), Longs.toArray(fileSplitSizeParts)));
                    fileSplitPathParts.clear();
                    fileSplitSizeParts.clear();
                }
                if (j == filePaths.length - 1 & fileSplitPathParts.size() != 0) {
                    splits.add(new CombineFileSplit(fileSplitPathParts.toArray(new Path[0]), Longs.toArray(fileSplitSizeParts)));
                }
                prevfilename = filename;
                j++;
            }
        }
        return splits;
    }
}

