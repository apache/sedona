/*
 * FILE: ShapeFileReader
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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser;

import java.io.IOException;

public class ShapeFileReader
        extends RecordReader<ShapeKey, ShpRecord>
{

    /**
     * record id
     */
    private ShapeKey recordKey = null;

    /**
     * primitive bytes value
     */
    private ShpRecord recordContent = null;

    /**
     * inputstream for .shp file
     */
    private FSDataInputStream shpInputStream = null;

    /**
     * file parser
     */
    ShpFileParser parser = null;

    /**
     * Iterator of indexes of records
     */
    private int[] indexes;

    /**
     * whether use index, true when using indexes
     */
    private boolean useIndex = false;

    /**
     * current index id
     */
    private int indexId = 0;

    /**
     * empty constructor
     */
    public ShapeFileReader()
    {
    }

    /**
     * constructor with index
     *
     * @param indexes
     */
    public ShapeFileReader(int[] indexes)
    {
        this.indexes = indexes;
        useIndex = true;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit) split;
        Path filePath = fileSplit.getPath();
        FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
        shpInputStream = fileSys.open(filePath);
        //assign inputstream to parser and parse file header to init;
        parser = new ShpFileParser(shpInputStream);
        parser.parseShapeFileHead();
    }

    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
        if (useIndex) {
            /**
             * with index, iterate until end and extract bytes with information from indexes
             */
            if (indexId == indexes.length) { return false; }
            // check offset, if current offset in inputStream not match with information in shx, move it
            if (shpInputStream.getPos() < indexes[indexId] * 2) {
                shpInputStream.skip(indexes[indexId] * 2 - shpInputStream.getPos());
            }
            int currentLength = indexes[indexId + 1] * 2 - 4;
            recordKey = new ShapeKey();
            recordKey.setIndex(parser.parseRecordHeadID());
            recordContent = parser.parseRecordPrimitiveContent(currentLength);
            indexId += 2;
            return true;
        }
        else {
            if (getProgress() >= 1) { return false; }
            recordKey = new ShapeKey();
            recordKey.setIndex(parser.parseRecordHeadID());
            recordContent = parser.parseRecordPrimitiveContent();
            return true;
        }
    }

    public ShapeKey getCurrentKey()
            throws IOException, InterruptedException
    {
        return recordKey;
    }

    public ShpRecord getCurrentValue()
            throws IOException, InterruptedException
    {
        return recordContent;
    }

    public float getProgress()
            throws IOException, InterruptedException
    {
        return parser.getProgress();
    }

    public void close()
            throws IOException
    {
        shpInputStream.close();
    }
}
