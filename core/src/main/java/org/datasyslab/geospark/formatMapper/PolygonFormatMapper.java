/**
 * FILE: PolygonFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PolygonFormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonFormatMapper.
 */
public class PolygonFormatMapper extends FormatMapper implements FlatMapFunction<Iterator<String>, Polygon> {
 
	
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

    @Override
    public Iterator<Polygon> call(Iterator<String> stringIterator) throws Exception {
        MultiPolygon multiSpatialObjects = null;
        LinearRing linear;
        int actualEndOffset;
        List result= new ArrayList<Polygon>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            try {
                switch (splitter) {
                    case CSV:
                        lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                        coordinatesList = new ArrayList<Coordinate>();
                        actualEndOffset = this.endOffset >= 0 ? this.endOffset : (lineSplitList.size() - 1);
                        for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
                            coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                        }
                        linear = fact.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                        spatialObject = new Polygon(linear, null, fact);
                        if (this.carryInputData) {
                            spatialObject.setUserData(line);
                        }
                        result.add((Polygon) spatialObject);
                        break;
                    case TSV:
                        lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                        coordinatesList = new ArrayList<Coordinate>();
                        actualEndOffset = this.endOffset >= 0 ? this.endOffset : (lineSplitList.size() - 1);
                        for (int i = this.startOffset; i <= actualEndOffset; i = i + 2) {
                            coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                        }
                        coordinates = new Coordinate[coordinatesList.size()];
                        coordinates = coordinatesList.toArray(coordinates);
                        linear = fact.createLinearRing(coordinates);
                        spatialObject = new Polygon(linear, null, fact);
                        if (this.carryInputData) {
                            spatialObject.setUserData(line);
                        }
                        result.add((Polygon) spatialObject);
                        break;
                    case GEOJSON:
                        GeoJSONReader reader = new GeoJSONReader();
                        if (line.contains("Feature")) {
                            Feature feature = (Feature) GeoJSONFactory.create(line);
                            spatialObject = reader.read(feature.getGeometry());
                        } else {
                            spatialObject = reader.read(line);
                        }
                        if (spatialObject instanceof MultiPolygon) {
                	/*
                	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects
                	 * and assign original input line to each object.
                	 */
                            multiSpatialObjects = (MultiPolygon) spatialObject;
                            for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                                spatialObject = multiSpatialObjects.getGeometryN(i);
                                if (this.carryInputData) {
                                    spatialObject.setUserData(line);
                                }
                                result.add((Polygon) spatialObject);
                            }
                        } else {
                            if (this.carryInputData) {
                                spatialObject.setUserData(line);
                            }
                            result.add((Polygon) spatialObject);
                        }
                        break;
                    case WKT:
                        lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                        WKTReader wktreader = new WKTReader();
                        spatialObject = wktreader.read(lineSplitList.get(this.startOffset));
                        if (spatialObject instanceof MultiPolygon) {
                            multiSpatialObjects = (MultiPolygon) spatialObject;
                            for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                    	/*
                    	 * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects
                    	 * and assign original input line to each object.
                    	 */
                                spatialObject = multiSpatialObjects.getGeometryN(i);
                                if (this.carryInputData) {
                                    spatialObject.setUserData(line);
                                }
                                result.add((Polygon) spatialObject);
                            }
                        } else {
                            if (this.carryInputData) {
                                spatialObject.setUserData(line);
                            }
                            result.add((Polygon) spatialObject);
                        }
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result.iterator();
    }
}