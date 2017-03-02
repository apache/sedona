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



public class UserSuppliedPointMapper implements FlatMapFunction<String, Object>{
    
    /** The spatial object. */
    Geometry spatialObject = null;
    
    /** The fact. */
    GeometryFactory fact = new GeometryFactory();
    
    /** The line split list. */
    List<String> lineSplitList;
    
    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    public Iterator call(String line) throws Exception {
        List result= new ArrayList<Point>();
        //Remove all quotes in the input line
        String lineNoQuote = line.replace("\"", "");
        List<String> lineSplitList;
        //Remove split the line by comma
        lineSplitList=Arrays.asList(lineNoQuote.split(","));
        double latitude = Double.parseDouble(lineSplitList.get(2));
        double longitude = Double.parseDouble(lineSplitList.get(3));
        spatialObject = fact.createPoint(new Coordinate(latitude,longitude));
        result.add(spatialObject);
        return result.iterator();
    }

}
