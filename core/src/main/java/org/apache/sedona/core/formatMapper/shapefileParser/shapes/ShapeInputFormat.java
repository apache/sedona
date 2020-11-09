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

package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
            CombineFileSplit combineChildPathFileSplit = (CombineFileSplit) super.getSplits(job).get(0);
            Path[] filePaths = combineChildPathFileSplit.getPaths();
            long[] fileSizes = combineChildPathFileSplit.getLengths();

            // sort by Path name and size using TreeMap
            Map<Path, Long> filePathSizePair = new TreeMap<Path, Long>();
            int i = 0;
            while (i < filePaths.length) {
                filePathSizePair.put(filePaths[i], fileSizes[i]);
                i++;
            }

            List<Path> fileSplitPathParts = new ArrayList<>();
            List<Long> fileSplitSizeParts = new ArrayList<>();
            String prevfilename = "";

            for (Path filePath : filePathSizePair.keySet()) {
                String filename = FilenameUtils.removeExtension(filePath.getName()).toLowerCase();
                fileSplitPathParts.add(filePath);
                fileSplitSizeParts.add(filePathSizePair.get(filePath));

                if (prevfilename != "" && !prevfilename.equals(filename)) {
                    // compare file name and if it is different then all same filename is into CombileFileSplit
                    splits.add(new CombineFileSplit(fileSplitPathParts.toArray(new Path[0]), Longs.toArray(fileSplitSizeParts)));
                    fileSplitPathParts.clear();
                    fileSplitSizeParts.clear();
                }
                prevfilename = filename;
            }

            if (fileSplitPathParts.size() != 0) {
                splits.add(new CombineFileSplit(fileSplitPathParts.toArray(new Path[0]), Longs.toArray(fileSplitSizeParts)));
            }
        }
        return splits;
    }
}

