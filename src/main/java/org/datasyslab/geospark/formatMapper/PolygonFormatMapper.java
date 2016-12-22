/**
 * FILE: PolygonFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PolygonFormatMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All right reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonFormatMapper.
 */
public class PolygonFormatMapper extends FormatMapper implements Function<String, Object> {
 
	
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
    public Polygon call(String line) throws Exception {
        Polygon polygon = null;
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
                polygon = new Polygon(linear, null, fact);
                if(this.carryInputData)
                {
                    polygon.setUserData(line);
                }
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
                polygon = new Polygon(linear, null, fact);
                if(this.carryInputData)
                {
                    polygon.setUserData(line);
                }
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                polygon = (Polygon) reader.read(line);
                if(this.carryInputData)
                {
                    polygon.setUserData(line);
                }
                break;
            case WKT:
            	lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wtkreader = new WKTReader();
                polygon = (Polygon) wtkreader.read(lineSplitList.get(this.startOffset));
                if(this.carryInputData)
                {
                    polygon.setUserData(line);
                }
                break;
        }
        return polygon;
    }

}