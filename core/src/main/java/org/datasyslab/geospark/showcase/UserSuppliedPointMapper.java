/**
 * FILE: UserSuppliedPointMapper.java
 * PATH: org.datasyslab.geospark.showcase.UserSuppliedPointMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.showcase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;



// TODO: Auto-generated Javadoc
/**
 * The Class UserSuppliedPointMapper.
 */
public class UserSuppliedPointMapper implements FlatMapFunction<Iterator<String>, Object>{
    
    /** The spatial object. */
    Geometry spatialObject = null;
    
    /** The fact. */
    GeometryFactory fact = new GeometryFactory();
    
    /** The line split list. */
    List<String> lineSplitList;

    @Override
    public Iterator<Object> call(Iterator<String> stringIterator) throws Exception {
        List result= new ArrayList<Point>();
        while(stringIterator.hasNext()) {
            String line = stringIterator.next();
            try {
                List<String> lineSplitList;
                //Split the line by comma
                lineSplitList = Arrays.asList(line.split(","));
                //Remove all quotes in the input line
                String latitudeString = lineSplitList.get(2).replaceAll("\"", "");
                String longitudeString = lineSplitList.get(3).replaceAll("\"", "");
                double latitude = Double.parseDouble(latitudeString);
                double longitude = Double.parseDouble(longitudeString);
                spatialObject = fact.createPoint(new Coordinate(longitude, latitude));
                result.add(spatialObject);
            } catch (Exception e) {
                //Get one error. The data probably is dirty. Just skip this line.
            }
        }
        return result.iterator();
    }
}
