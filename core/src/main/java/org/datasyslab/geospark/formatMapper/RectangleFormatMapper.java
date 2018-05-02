/*
 * FILE: RectangleFormatMapper
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GeometryType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RectangleFormatMapper
        extends FormatMapper
        implements FlatMapFunction<Iterator<String>, Polygon>
{

    /**
     * Instantiates a new rectangle format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public RectangleFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, 3, Splitter, carryInputData, GeometryType.RECTANGLE);
    }

    /**
     * Instantiates a new rectangle format mapper.
     *
     * @param startOffset the start offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public RectangleFormatMapper(Integer startOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, startOffset+3, Splitter, carryInputData, GeometryType.RECTANGLE);
    }

    @Override
    public Iterator<Polygon> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<Polygon> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            addGeometry(readGeometry(line), result);
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<Polygon> result)
    {
        if (geometry instanceof MultiPolygon) {
            addMultiGeometry((MultiPolygon) geometry, result);
        }
        else {
            result.add((Polygon) (geometry.getEnvelope()));
        }
    }
}
