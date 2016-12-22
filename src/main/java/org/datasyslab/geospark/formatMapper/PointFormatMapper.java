/**
 * FILE: PointFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PointFormatMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All right reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PointFormatMapper.
 */
public class PointFormatMapper extends FormatMapper implements Function<String, Object> {


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
    public Point call(String line) throws Exception {
        Point point = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.startOffset)),
                        Double.parseDouble(lineSplitList.get(1 + this.startOffset)));
                point = fact.createPoint(coordinate);
                if(this.carryInputData)
                {
                    point.setUserData(line);
                }
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.startOffset)),
                        Double.parseDouble(lineSplitList.get(1 + this.startOffset)));
                point = fact.createPoint(coordinate);
                if(this.carryInputData)
                {
                    point.setUserData(line);
                }
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                point = (Point)reader.read(line);
                if(this.carryInputData)
                {
                    point.setUserData(line);
                }
                break;
            case WKT:
            	lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wktreader = new WKTReader();
                Envelope envelope=wktreader.read(lineSplitList.get(this.startOffset)).getEnvelopeInternal();
                coordinate = new Coordinate (envelope.getMinX(),envelope.getMinY());
                point = fact.createPoint(coordinate);
                coordinate = new Coordinate (85.01,34.01);
                point = fact.createPoint(coordinate);
                if(this.carryInputData)
                {
                    point.setUserData(line);
                }
                break;
        }
        return point;
    }
}
