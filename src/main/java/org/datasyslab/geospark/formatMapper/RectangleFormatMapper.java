/**
 * FILE: RectangleFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.RectangleFormatMapper.java
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
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

/**
 * The Class RectangleFormatMapper.
 */
public class RectangleFormatMapper extends FormatMapper implements FlatMapFunction<String,Object>
{

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
	}

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	public List call(String line) throws Exception {
		MultiPolygon multiSpatialObjects = null;
    	List result= new ArrayList<Polygon>();
		Double x1,x2,y1,y2;
        LinearRing linear;
		switch (splitter) {
			case CSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(this.startOffset));
				x2 = Double.parseDouble(lineSplitList.get(this.startOffset + 2));
				y1 = Double.parseDouble(lineSplitList.get(this.startOffset + 1));
				y2 = Double.parseDouble(lineSplitList.get(this.startOffset + 3));
		        coordinates = new Coordinate[5];
		        coordinates[0]=new Coordinate(x1,y1);
		        coordinates[1]=new Coordinate(x1,y2);
		        coordinates[2]=new Coordinate(x2,y2);
		        coordinates[3]=new Coordinate(x2,y1);
		        coordinates[4]=coordinates[0];
                linear = fact.createLinearRing(coordinates);
                spatialObject = new Polygon(linear, null, fact);
				if(this.carryInputData)
				{
					spatialObject.setUserData(line);
				}
				result.add(spatialObject);
				break;
			case TSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(this.startOffset));
				x2 = Double.parseDouble(lineSplitList.get(this.startOffset + 2));
				y1 = Double.parseDouble(lineSplitList.get(this.startOffset + 1));
				y2 = Double.parseDouble(lineSplitList.get(this.startOffset + 3));
		        coordinates = new Coordinate[5];
		        coordinates[0]=new Coordinate(x1,y1);
		        coordinates[1]=new Coordinate(x1,y2);
		        coordinates[2]=new Coordinate(x2,y2);
		        coordinates[3]=new Coordinate(x2,y1);
		        coordinates[4]=coordinates[0];
                linear = fact.createLinearRing(coordinates);
                spatialObject = new Polygon(linear, null, fact);
				if(this.carryInputData)
				{
					spatialObject.setUserData(line);
				}
				result.add(spatialObject);
				break;
			case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                spatialObject = reader.read(line);
                if(spatialObject instanceof MultiPolygon)
                {
                	/*
                	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects 
                	 * and assign original input line to each object.
                	 */
                	multiSpatialObjects = (MultiPolygon) spatialObject;
                	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
                	{
                		spatialObject = multiSpatialObjects.getGeometryN(i);
        				x1 = spatialObject.getEnvelopeInternal().getMinX();
        				x2 = spatialObject.getEnvelopeInternal().getMaxX();
        				y1 = spatialObject.getEnvelopeInternal().getMinY();
        				y2 = spatialObject.getEnvelopeInternal().getMaxY();
        		        coordinates = new Coordinate[5];
        		        coordinates[0]=new Coordinate(x1,y1);
        		        coordinates[1]=new Coordinate(x1,y2);
        		        coordinates[2]=new Coordinate(x2,y2);
        		        coordinates[3]=new Coordinate(x2,y1);
        		        coordinates[4]=coordinates[0];
                        linear = fact.createLinearRing(coordinates);
                        spatialObject = new Polygon(linear, null, fact);
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add(spatialObject);
                	}
                }
                else
                {
    				x1 = spatialObject.getEnvelopeInternal().getMinX();
    				x2 = spatialObject.getEnvelopeInternal().getMaxX();
    				y1 = spatialObject.getEnvelopeInternal().getMinY();
    				y2 = spatialObject.getEnvelopeInternal().getMaxY();
    		        coordinates = new Coordinate[5];
    		        coordinates[0]=new Coordinate(x1,y1);
    		        coordinates[1]=new Coordinate(x1,y2);
    		        coordinates[2]=new Coordinate(x2,y2);
    		        coordinates[3]=new Coordinate(x2,y1);
    		        coordinates[4]=coordinates[0];
                    linear = fact.createLinearRing(coordinates);
                    spatialObject = new Polygon(linear, null, fact);
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
            		result.add(spatialObject);
                }
                break;
			case WKT:
            	lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wktreader = new WKTReader();
                spatialObject = wktreader.read(lineSplitList.get(this.startOffset));
                if(spatialObject instanceof MultiPolygon)
                {
                	multiSpatialObjects = (MultiPolygon) spatialObject;
                	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
                	{
                    	/*
                    	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects 
                    	 * and assign original input line to each object.
                    	 */
                		spatialObject = multiSpatialObjects.getGeometryN(i);
        				x1 = spatialObject.getEnvelopeInternal().getMinX();
        				x2 = spatialObject.getEnvelopeInternal().getMaxX();
        				y1 = spatialObject.getEnvelopeInternal().getMinY();
        				y2 = spatialObject.getEnvelopeInternal().getMaxY();
        		        coordinates = new Coordinate[5];
        		        coordinates[0]=new Coordinate(x1,y1);
        		        coordinates[1]=new Coordinate(x1,y2);
        		        coordinates[2]=new Coordinate(x2,y2);
        		        coordinates[3]=new Coordinate(x2,y1);
        		        coordinates[4]=coordinates[0];
                        linear = fact.createLinearRing(coordinates);
                        spatialObject = new Polygon(linear, null, fact);
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add(spatialObject);
                	}
                }
                else
                {
    				x1 = spatialObject.getEnvelopeInternal().getMinX();
    				x2 = spatialObject.getEnvelopeInternal().getMaxX();
    				y1 = spatialObject.getEnvelopeInternal().getMinY();
    				y2 = spatialObject.getEnvelopeInternal().getMaxY();
    		        coordinates = new Coordinate[5];
    		        coordinates[0]=new Coordinate(x1,y1);
    		        coordinates[1]=new Coordinate(x1,y2);
    		        coordinates[2]=new Coordinate(x2,y2);
    		        coordinates[3]=new Coordinate(x2,y1);
    		        coordinates[4]=coordinates[0];
                    linear = fact.createLinearRing(coordinates);
                    spatialObject = new Polygon(linear, null, fact);
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
            		result.add(spatialObject);
                }
                break;
		}
		return result;
	}
}
