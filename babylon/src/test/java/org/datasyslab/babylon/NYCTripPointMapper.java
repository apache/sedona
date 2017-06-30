/**
 * FILE: NYCTripPointMapper.java
 * PATH: org.datasyslab.babylon.NYCTripPointMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.babylon;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class NYCTripPointMapper.
 */
class NYCTripPointMapper implements FlatMapFunction<String, Object> {
	 
	/** The result. */
	List<Point> result= new ArrayList<Point>();
    
    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    public List call(String line) throws Exception {
        try {
            List result= new ArrayList<LineString>();
            Geometry spatialObject = null;
            List<String> lineSplitList = Arrays.asList(line.split(","));
            Coordinate coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(5)),Double.parseDouble(lineSplitList.get(6)));
            GeometryFactory geometryFactory = new GeometryFactory();
            result.add(geometryFactory.createPoint(coordinate));
            return result;
        }
        catch (NumberFormatException e)
        {
            return result;
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            System.out.println(line);
            //e.printStackTrace();
        }

        return result;
    }

} 

