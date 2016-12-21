/**
 * FILE: RectangleFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.RectangleFormatMapper.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleFormatMapper.
 */
public class RectangleFormatMapper implements Serializable,Function<String,Object>
{
	
	/** The offset. */
	Integer offset = 0;
	
	/** The splitter. */
	FileDataSplitter splitter = FileDataSplitter.CSV;

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param Offset the offset
	 * @param Splitter the splitter
	 */
	public RectangleFormatMapper(Integer Offset, FileDataSplitter Splitter) {
		this.offset = Offset;
		this.splitter = Splitter;
	}

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param Splitter the splitter
	 */
	public RectangleFormatMapper( FileDataSplitter Splitter) {
		this.offset = 0;
		this.splitter = Splitter;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	public Envelope call(String line) throws Exception {
		Envelope rectangle = null;
		GeometryFactory fact = new GeometryFactory();
		List<String> lineSplitList;
		Coordinate coordinate;
		Double x1,x2,y1,y2;
		switch (splitter) {
			case CSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				rectangle.setUserData(line);
				break;
			case TSV:
				lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				rectangle.setUserData(line);
				break;
			case GEOJSON:
				GeoJSONReader reader = new GeoJSONReader();
				Geometry result = reader.read(line);
				rectangle =result.getEnvelopeInternal();
				rectangle.setUserData(line);
				break;
			case WKT:
				lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
				WKTReader wtkreader = new WKTReader();
				rectangle = wtkreader.read(lineSplitList.get(offset)).getEnvelopeInternal();
				rectangle.setUserData(line);
				break;
			default:
				throw new Exception("Input type not recognized, ");
		}
		return rectangle;
	}
}
