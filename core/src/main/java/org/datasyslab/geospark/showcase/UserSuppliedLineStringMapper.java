/*
 * FILE: UserSuppliedLineStringMapper
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
package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class UserSuppliedLineStringMapper.
 */
public class UserSuppliedLineStringMapper
        implements FlatMapFunction<Iterator<String>, Object>
{

    /**
     * The spatial object.
     */
    Geometry spatialObject = null;

    /**
     * The multi spatial objects.
     */
    MultiPolygon multiSpatialObjects = null;

    /**
     * The fact.
     */
    GeometryFactory fact = new GeometryFactory();

    /**
     * The line split list.
     */
    List<String> lineSplitList;

    /**
     * The coordinates list.
     */
    ArrayList<Coordinate> coordinatesList;

    /**
     * The coordinates.
     */
    Coordinate[] coordinates;

    /**
     * The linear.
     */
    LinearRing linear;

    /**
     * The actual end offset.
     */
    int actualEndOffset;

    @Override
    public Iterator<Object> call(Iterator<String> stringIterator)
            throws Exception
    {
        List result = new ArrayList<LineString>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            Geometry spatialObject = null;
            MultiLineString multiSpatialObjects = null;
            List<String> lineSplitList;
            lineSplitList = Arrays.asList(line.split("\t"));
            String newLine = lineSplitList.get(0).replace("\"", "");
            WKTReader wktreader = new WKTReader();
            spatialObject = wktreader.read(newLine);
            if (spatialObject instanceof MultiLineString) {
                multiSpatialObjects = (MultiLineString) spatialObject;
                for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                    spatialObject = multiSpatialObjects.getGeometryN(i);
                    result.add((LineString) spatialObject);
                }
            }
            else {
                result.add((LineString) spatialObject);
            }
        }
        return result.iterator();
    }
}
