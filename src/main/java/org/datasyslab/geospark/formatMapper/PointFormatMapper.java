/**
 * FILE: PointFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PointFormatMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PointFormatMapper.
 */
public class PointFormatMapper extends FormatMapper implements FlatMapFunction<String, Object> {


	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Iterator call(String line) throws Exception {
        List result= new ArrayList<Point>();
        Geometry spatialObject = null;
    	MultiPoint multiSpatialObjects = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.startOffset)),
                        Double.parseDouble(lineSplitList.get(1 + this.startOffset)));
                spatialObject = fact.createPoint(coordinate);
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((Point)spatialObject);
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.startOffset)),
                        Double.parseDouble(lineSplitList.get(1 + this.startOffset)));
                spatialObject = fact.createPoint(coordinate);
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((Point)spatialObject);
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                spatialObject = reader.read(line);
                if(spatialObject instanceof MultiPoint)
                {
                	/*
                	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects 
                	 * and assign original input line to each object.
                	 */
                	multiSpatialObjects = (MultiPoint) spatialObject;
                	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
                	{
                		spatialObject = multiSpatialObjects.getGeometryN(i);
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add((Point) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((Point)spatialObject);
                }
                break;
            case WKT:
            	lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wktreader = new WKTReader();
                spatialObject = wktreader.read(lineSplitList.get(this.startOffset));
                if(spatialObject instanceof MultiPoint)
                {
                	multiSpatialObjects = (MultiPoint) spatialObject;
                	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
                	{
                    	/*
                    	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects 
                    	 * and assign original input line to each object.
                    	 */
                		spatialObject = multiSpatialObjects.getGeometryN(i);
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add((Point) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((Point)spatialObject);
                }
                break;
        }
        return result.iterator();
    }
}
