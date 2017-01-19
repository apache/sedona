/**
 * FILE: LineStringFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.LineStringFormatMapper.java
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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class LineStringFormatMapper.
 */
public class LineStringFormatMapper extends FormatMapper implements FlatMapFunction<String, Object> {
	
	/**
	 * Instantiates a new line string format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public LineStringFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Instantiates a new line string format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public LineStringFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}
	
	/* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Iterator call(String line) throws Exception {
    	List result= new ArrayList<LineString>();
        Geometry spatialObject = null;
        MultiLineString multiSpatialObjects = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        ArrayList<Coordinate> coordinatesList;
        int actualEndOffset;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinatesList = new ArrayList<Coordinate>();
                actualEndOffset = this.endOffset>=0?this.endOffset:(lineSplitList.size()-1);
                for (int i = this.startOffset; i <= actualEndOffset; i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                spatialObject = fact.createLineString(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((LineString)spatialObject);
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinatesList = new ArrayList<Coordinate>();
                actualEndOffset = this.endOffset>=0?this.endOffset:(lineSplitList.size()-1);
                for (int i = this.startOffset; i <= actualEndOffset; i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                spatialObject = fact.createLineString(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((LineString)spatialObject);
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                spatialObject = reader.read(line);
                if(spatialObject instanceof MultiLineString)
                {
                	/*
                	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects 
                	 * and assign original input line to each object.
                	 */
                	multiSpatialObjects = (MultiLineString) spatialObject;
                	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
                	{
                		spatialObject = multiSpatialObjects.getGeometryN(i);
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add((LineString) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((LineString)spatialObject);
                }
                break;
            case WKT:
            	lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wktreader = new WKTReader();
                spatialObject = wktreader.read(lineSplitList.get(this.startOffset));
                if(spatialObject instanceof MultiLineString)
                {
                	multiSpatialObjects = (MultiLineString) spatialObject;
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
                		result.add((LineString) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((LineString)spatialObject);
                }
                break;
        }
        return result.iterator();
    }


}