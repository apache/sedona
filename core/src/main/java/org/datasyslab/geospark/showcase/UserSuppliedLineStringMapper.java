/**
 * FILE: UserSuppliedLineStringMapper.java
 * PATH: org.datasyslab.geospark.showcase.UserSuppliedLineStringMapper.java
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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class UserSuppliedLineStringMapper.
 */
public class UserSuppliedLineStringMapper implements FlatMapFunction<Iterator<String>, Object>{
    
    /** The spatial object. */
    Geometry spatialObject = null;
    
    /** The multi spatial objects. */
    MultiPolygon multiSpatialObjects = null;
    
    /** The fact. */
    GeometryFactory fact = new GeometryFactory();
    
    /** The line split list. */
    List<String> lineSplitList;
    
    /** The coordinates list. */
    ArrayList<Coordinate> coordinatesList;
    
    /** The coordinates. */
    Coordinate[] coordinates;
    
    /** The linear. */
    LinearRing linear;
    
    /** The actual end offset. */
    int actualEndOffset;

    @Override
    public Iterator<Object> call(Iterator<String> stringIterator) throws Exception {
        List result= new ArrayList<LineString>();
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
            } else {
                result.add((LineString) spatialObject);
            }
        }
            return result.iterator();
        }
}
