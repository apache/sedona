package org.datasyslab.geospark.formatMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class PolygonFormatMapper implements Function<String, Polygon>, Serializable {
    Integer offset = 0;
    String splitter = "csv";

    public PolygonFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public PolygonFormatMapper(String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public Polygon call(String line) throws Exception {
        Polygon polygon = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        ArrayList<Coordinate> coordinatesList;
        Coordinate[] coordinates;
        LinearRing linear;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                linear = fact.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                polygon = new Polygon(linear, null, fact);
                break;
            case "tsv":
                lineSplitList = Arrays.asList(line.split("\t"));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i = i + 2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                coordinates = new Coordinate[coordinatesList.size()];
                coordinates = coordinatesList.toArray(coordinates);
                linear = fact.createLinearRing(coordinates);
                polygon = new Polygon(linear, null, fact);
                break;
            case "geojson":
                GeoJSONReader reader = new GeoJSONReader();
                polygon = (Polygon) reader.read(line);
                break;
            case "wkt":
            	lineSplitList=Arrays.asList(line.split("\t"));
                WKTReader wtkreader = new WKTReader();
                try {
                    polygon = (Polygon) wtkreader.read(lineSplitList.get(offset));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return polygon;
    }
}