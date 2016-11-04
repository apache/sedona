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
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTReader;

public class RectangleFormatMapper implements Serializable,Function<String,Envelope>
{
	Integer offset = 0;
	FileDataSplitter splitter = FileDataSplitter.CSV;

	public RectangleFormatMapper(Integer Offset, FileDataSplitter Splitter) {
		this.offset = Offset;
		this.splitter = Splitter;
	}

	public RectangleFormatMapper( FileDataSplitter Splitter) {
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
			case CSV:
				lineSplitList = Arrays.asList(line.split(splitter.getSplitter()));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				rectangle.setUserData(line);
				break;
			case TSV:
				lineSplitList = Arrays.asList(line.split(splitter.getSplitter()));
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
				lineSplitList=Arrays.asList(line.split(splitter.getSplitter()));
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
