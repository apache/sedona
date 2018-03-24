/**
 * FILE: UserSuppliedPointMapper.java
 * PATH: org.datasyslab.geospark.showcase.UserSuppliedPointMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UserSuppliedPointMapper
        implements FlatMapFunction<Iterator<String>, Point>
{

    /**
     * The factory.
     */
    private final GeometryFactory factory = new GeometryFactory();

    @Override
    public Iterator<Point> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<Point> result = new ArrayList<Point>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            try {
                //Split the line by comma
                String[] columns = line.split(",");
                //Remove all quotes in the input line
                String latitudeString = columns[2].replaceAll("\"", "");
                String longitudeString = columns[3].replaceAll("\"", "");
                double latitude = Double.parseDouble(latitudeString);
                double longitude = Double.parseDouble(longitudeString);
                Point point = factory.createPoint(new Coordinate(longitude, latitude));
                result.add(point);
            }
            catch (Exception e) {
                //Get one error. The data probably is dirty. Just skip this line.
            }
        }
        return result.iterator();
    }
}
