/*
 * FILE: BoundaryInputFormat
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
package org.datasyslab.geospark.formatMapper.shapefileParser.boundary;

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

public class BoundaryInputFormat
        extends CombineFileInputFormat<Long, BoundBox>
{
    @Override
    public RecordReader<Long, BoundBox> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException
    {
        return new BoundaryRecordReader();
    }

    /**
     * enforce isSplitable to be false so that super.getSplits() combine all files as one split.
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

    /**
     * get and combine all splits of .shp files
     *
     * @param job
     * @return
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job)
            throws IOException
    {
        // get original combine split.
        CombineFileSplit combineSplit = (CombineFileSplit) super.getSplits(job).get(0);
        Path[] paths = combineSplit.getPaths();

        // get indexes of all .shp file
        List<Integer> shpIds = new ArrayList<>();
        for (int i = 0; i < paths.length; ++i) {
            if (FilenameUtils.getExtension(paths[i].toString()).equals("shp")) {
                shpIds.add(i);
            }
        }

        // prepare parameters for constructing new combine split
        Path[] shpPaths = new Path[shpIds.size()];
        long[] shpStarts = new long[shpIds.size()];
        long[] shpLengths = new long[shpIds.size()];

        for (int i = 0; i < shpIds.size(); ++i) {
            int id = shpIds.get(i);
            shpPaths[i] = combineSplit.getPath(id);
            shpStarts[i] = combineSplit.getOffset(id);
            shpLengths[i] = combineSplit.getLength(id);
        }

        //combine all .shp splits as one split.
        CombineFileSplit shpSplit = new CombineFileSplit(shpPaths, shpStarts, shpLengths, combineSplit.getLocations());
        List<InputSplit> shpSplits = new ArrayList<>();
        shpSplits.add(shpSplit);
        return shpSplits;
    }
}
