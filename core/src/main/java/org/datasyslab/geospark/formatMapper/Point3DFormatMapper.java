/*
 * FILE: Point3DFormatMapper
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
package org.datasyslab.geospark.formatMapper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.formatMapper.FormatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class Point3DFormatMapper
        extends FormatMapper
        implements FlatMapFunction<Iterator<String>, Point3D>
{

    /**
     * Instantiates a new point3D format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public Point3DFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(Splitter, carryInputData);
    }

    /**
     * Instantiates a new point3D format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public Point3DFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter, boolean carryInputData)
    {
        super(startOffset, endOffset, Splitter, carryInputData);
    }


    @Override
    public Iterator<Point3D> call(final Iterator<String> stringIterator)
            throws Exception
    {
        List<Point3D> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            String[] columns = line.split(splitter.getDelimiter());
            String time = columns[1];
            time = time.replace("T", " ");
            time = time.replace("Z", "");
            DateFormat format =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            double z;
            try {
                Date date = format.parse(time);
                z = date.getTime();
            } catch (java.text.ParseException e) {
                e.printStackTrace();
                throw new ParseException(e);
            }
            Coordinate coordinate = new Coordinate(
                    Double.parseDouble(columns[this.startOffset]),
                    Double.parseDouble(columns[1 + this.startOffset]));
            Point point = factory.createPoint(coordinate);
            if (this.carryInputData) {
                point.setUserData(line);
            }
            Point3D point3D = new Point3D(point, z);
            result.add(point3D);

        }
        return result.iterator();
    }

}
