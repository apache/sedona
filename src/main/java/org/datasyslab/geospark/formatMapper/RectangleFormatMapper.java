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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
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
		// TODO Auto-generated constructor stub
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
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	public Iterator call(String line) throws Exception {
		Envelope rectangle = null;
		List result= new ArrayList<Envelope>();
		Geometry spatialObject = null;
		MultiPolygon multiSpatialObjects = null;
		List<String> lineSplitList;
		Double x1,x2,y1,y2;
		switch (splitter) {
			case CSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(this.startOffset));
				x2 = Double.parseDouble(lineSplitList.get(this.startOffset + 2));
				y1 = Double.parseDouble(lineSplitList.get(this.startOffset + 1));
				y2 = Double.parseDouble(lineSplitList.get(this.startOffset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				if(this.carryInputData)
				{
					rectangle.setUserData(line);
				}
				result.add(rectangle);
				break;
			case TSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(this.startOffset));
				x2 = Double.parseDouble(lineSplitList.get(this.startOffset + 2));
				y1 = Double.parseDouble(lineSplitList.get(this.startOffset + 1));
				y2 = Double.parseDouble(lineSplitList.get(this.startOffset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				if(this.carryInputData)
				{
					rectangle.setUserData(line);
				}
				result.add(rectangle);
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
                		result.add(spatialObject.getEnvelopeInternal());
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
            		result.add(spatialObject.getEnvelopeInternal());
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
                		result.add(spatialObject.getEnvelopeInternal());
                	}
                }
                else
                {
                    if(this.carryInputData)
                    {
                    	spatialObject.setUserData(line);
                    }
            		result.add(spatialObject.getEnvelopeInternal());
                }
                break;
		}
		return result.iterator();
	}
}
