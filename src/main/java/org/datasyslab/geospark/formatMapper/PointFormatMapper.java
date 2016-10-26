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
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class PointFormatMapper implements Serializable, Function<String, Point> {
    Integer offset = 0;
    String splitter = "csv";

    public PointFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public PointFormatMapper( String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public Point call(String line) throws Exception {
        Point point = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            case "tsv":
                lineSplitList = Arrays.asList(line.split("\t"));
                coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            case "geojson":
                GeoJSONReader reader = new GeoJSONReader();
                point = (Point)reader.read(line);
                point.setUserData(line);
                break;
            case "wkt":
            	lineSplitList = Arrays.asList(line.split("\t"));
                WKTReader wktreader = new WKTReader();
                Envelope envelope=wktreader.read(lineSplitList.get(offset)).getEnvelopeInternal();
                coordinate = new Coordinate (envelope.getMinX(),envelope.getMinY());
                point = fact.createPoint(coordinate);
                coordinate = new Coordinate (85.01,34.01);
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return point;
    }
}
