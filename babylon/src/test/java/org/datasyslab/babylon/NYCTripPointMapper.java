/**
 * FILE: NYCTripTest.java
 * PATH: org.datasyslab.babylon.NYCTripTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class NYCTripPointMapper implements FlatMapFunction<String, Object> {
	 
	List<Point> result= new ArrayList<Point>();
    public Iterator call(String line) throws Exception {
        try {
            List result= new ArrayList<LineString>();
            Geometry spatialObject = null;
            List<String> lineSplitList = Arrays.asList(line.split(","));
            Coordinate coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(5)),Double.parseDouble(lineSplitList.get(6)));
            GeometryFactory geometryFactory = new GeometryFactory();
            result.add(geometryFactory.createPoint(coordinate));
            return result.iterator();
        }
        catch (NumberFormatException e)
        {
            return result.iterator();
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            System.out.println(line);
            //e.printStackTrace();
        }

        return result.iterator();
    }

} 

