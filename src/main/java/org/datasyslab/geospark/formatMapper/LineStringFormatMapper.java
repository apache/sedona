/**
 * FILE: LineStringFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.LineStringFormatMapper.java
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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class LineStringFormatMapper.
 */
public class LineStringFormatMapper extends FormatMapper implements Function<String, Object> {
	
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
    public LineString call(String line) throws Exception {
        LineString lineString = null;
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
                lineString = fact.createLineString(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                if(this.carryInputData)
                {
                    lineString.setUserData(line);
                }
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinatesList = new ArrayList<Coordinate>();
                actualEndOffset = this.endOffset>=0?this.endOffset:(lineSplitList.size()-1);
                for (int i = this.startOffset; i <= actualEndOffset; i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                lineString = fact.createLineString(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                if(this.carryInputData)
                {
                    lineString.setUserData(line);
                }
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                lineString = (LineString) reader.read(line);
                if(this.carryInputData)
                {
                    lineString.setUserData(line);
                }
                break;
            case WKT:
            	lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wtkreader = new WKTReader();
                lineString = (LineString) wtkreader.read(lineSplitList.get(this.startOffset));
                if(this.carryInputData)
                {
                    lineString.setUserData(line);
                }
                break;
        }
        return lineString;
    }


}