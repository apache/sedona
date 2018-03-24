/**
 * FILE: NYCTripPointMapper.java
 * PATH: org.datasyslab.geosparkviz.NYCTripPointMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
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

