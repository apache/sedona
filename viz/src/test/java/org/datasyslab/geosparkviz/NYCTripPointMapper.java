/*
 * FILE: NYCTripPointMapper
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
package org.datasyslab.geosparkviz;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class NYCTripPointMapper.
 */
class NYCTripPointMapper
        implements FlatMapFunction<String, Object>
{

    /**
     * The result.
     */
    List<Point> result = new ArrayList<Point>();

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    public Iterator call(String line)
            throws Exception
    {
        try {
            List result = new ArrayList<LineString>();
            Geometry spatialObject = null;
            List<String> lineSplitList = Arrays.asList(line.split(","));
            Coordinate coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(5)), Double.parseDouble(lineSplitList.get(6)));
            GeometryFactory geometryFactory = new GeometryFactory();
            result.add(geometryFactory.createPoint(coordinate));
            return result.iterator();
        }
        catch (NumberFormatException e) {
            return result.iterator();
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(line);
            //e.printStackTrace();
        }

        return result.iterator();
    }
}

