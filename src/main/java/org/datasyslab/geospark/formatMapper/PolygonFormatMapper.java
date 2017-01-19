/**
 * FILE: PolygonFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PolygonFormatMapper.java
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
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonFormatMapper.
 */
public class PolygonFormatMapper extends FormatMapper implements FlatMapFunction<String, Object> {
 
	
    /**
     * Instantiates a new polygon format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PolygonFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Instantiates a new polygon format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PolygonFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Iterator call(String line) throws Exception {
        List result= new ArrayList<Polygon>();
        Geometry spatialObject = null;
        MultiPolygon multiSpatialObjects = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        ArrayList<Coordinate> coordinatesList;
        Coordinate[] coordinates;
        LinearRing linear;
        int actualEndOffset;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinatesList = new ArrayList<Coordinate>();
                actualEndOffset = this.endOffset>=0?this.endOffset:(lineSplitList.size()-1);
                for (int i = this.startOffset; i <= actualEndOffset; i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                linear = fact.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                spatialObject = new Polygon(linear, null, fact);
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((Polygon)spatialObject);
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinatesList = new ArrayList<Coordinate>();
                actualEndOffset = this.endOffset>=0?this.endOffset:(lineSplitList.size()-1);
                for (int i = this.startOffset; i <= actualEndOffset; i = i + 2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                coordinates = new Coordinate[coordinatesList.size()];
                coordinates = coordinatesList.toArray(coordinates);
                linear = fact.createLinearRing(coordinates);
                spatialObject = new Polygon(linear, null, fact);
                if(this.carryInputData)
                {
                	spatialObject.setUserData(line);
                }
                result.add((Polygon)spatialObject);
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
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add((Polygon) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((Polygon)spatialObject);
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
                		if(this.carryInputData)
                		{
                    		spatialObject.setUserData(line);
                		}
                		result.add((Polygon) spatialObject);
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
                    result.add((Polygon)spatialObject);
                }
                break;
        }
        return result.iterator();
    }

}