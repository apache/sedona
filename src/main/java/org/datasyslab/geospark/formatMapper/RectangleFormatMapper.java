/**
 * FILE: RectangleFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.RectangleFormatMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All right reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleFormatMapper.
 */
public class RectangleFormatMapper extends FormatMapper implements Function<String,Object>
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
	public Envelope call(String line) throws Exception {
		Envelope rectangle = null;
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
				break;
			case GEOJSON:
				GeoJSONReader reader = new GeoJSONReader();
				Geometry result = reader.read(line);
				rectangle =result.getEnvelopeInternal();
				if(this.carryInputData)
				{
					rectangle.setUserData(line);
				}
				break;
			case WKT:
				lineSplitList=Arrays.asList(line.split(splitter.getDelimiter()));
				WKTReader wtkreader = new WKTReader();
				rectangle = wtkreader.read(lineSplitList.get(this.startOffset)).getEnvelopeInternal();
				if(this.carryInputData)
				{
					rectangle.setUserData(line);
				}
				break;
		}
		return rectangle;
	}
}
