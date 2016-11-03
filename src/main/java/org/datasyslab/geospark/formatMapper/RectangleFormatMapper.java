package org.datasyslab.geospark.formatMapper;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class RectangleFormatMapper implements Serializable,Function<String,Envelope>
{
	Integer offset = 0;
	String splitter = "csv";

	public RectangleFormatMapper(Integer Offset, String Splitter) {
		this.offset = Offset;
		this.splitter = Splitter;
	}

	public RectangleFormatMapper( String Splitter) {
		this.offset = 0;
		this.splitter = Splitter;
	}

	public Envelope call(String line) throws Exception {
		Envelope rectangle = null;
		GeometryFactory fact = new GeometryFactory();
		List<String> lineSplitList;
		Coordinate coordinate;
		Double x1,x2,y1,y2;
		switch (splitter) {
			case "csv":
				lineSplitList = Arrays.asList(line.split(","));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				rectangle.setUserData(line);
				break;
			case "tsv":
				lineSplitList = Arrays.asList(line.split("\t"));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				rectangle.setUserData(line);
				break;
			case "geojson":
				GeoJSONReader reader = new GeoJSONReader();
				Geometry result = reader.read(line);
				rectangle =result.getEnvelopeInternal();
				rectangle.setUserData(line);
				break;
			case "wkt":
				lineSplitList=Arrays.asList(line.split("\t"));
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
