/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common;

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.FormatUtils;
import org.apache.sedona.common.utils.GeoHashDecoder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.gml2.GMLReader;
import org.locationtech.jts.io.kml.KMLReader;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class Constructors {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    public static Geometry geomFromWKT(String wkt, int srid) throws ParseException {
        if (wkt == null) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }

    public static Geometry geomFromWKB(byte[] wkb) throws ParseException {
        return new WKBReader().read(wkb);
    }

    public static Geometry mLineFromText(String wkt, int srid) throws ParseException {
        if (wkt == null || !wkt.startsWith("MULTILINESTRING")) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }

    public static Geometry mPolyFromText(String wkt, int srid) throws ParseException {
        if (wkt == null || !wkt.startsWith("MULTIPOLYGON")) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }


    /**
     * Creates a point from the given coordinate.
     * ST_Point in Sedona Spark API took an optional z value before v1.4.0.
     * This was removed to avoid confusion with other GIS implementations where the optional third argument is srid.
     *
     * A future version of Sedona will add a srid parameter once enough users have upgraded and hence are forced
     * to use ST_PointZ for 3D points.
     *
     * @param x the x value
     * @param y the y value
     * @return The point geometry
     */
    public static Geometry point(double x, double y) {
        // See srid parameter discussion in https://issues.apache.org/jira/browse/SEDONA-234
        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    /**
     * Creates a point from the given coordinate.
     *
     * @param x the x value
     * @param y the y value
     * @param z the z value
     * @param srid Set to 0 if unknown
     * @return The point geometry
     */
    public static Geometry pointZ(double x, double y, double z, int srid) {
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return geometryFactory.createPoint(new Coordinate(x, y, z));
    }

    public static Geometry geomFromText(String geomString, String geomFormat, GeometryType geometryType) {
        FileDataSplitter fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat);
        FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false, geometryType);
        try {
            return formatMapper.readGeometry(geomString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Geometry geomFromText(String geomString, FileDataSplitter fileDataSplitter) {
        FormatUtils<Geometry> formatMapper = new FormatUtils<>(fileDataSplitter, false);
        try {
            return formatMapper.readGeometry(geomString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Geometry pointFromText(String geomString, String geomFormat) {
        return geomFromText(geomString, geomFormat, GeometryType.POINT);
    }

    public static Geometry polygonFromText(String geomString, String geomFormat) {
        return geomFromText(geomString, geomFormat, GeometryType.POLYGON);
    }

    public static Geometry lineStringFromText(String geomString, String geomFormat) {
        return geomFromText(geomString, geomFormat, GeometryType.LINESTRING);
    }

    public static Geometry lineFromText(String geomString) {
        FileDataSplitter fileDataSplitter = FileDataSplitter.WKT;
        Geometry geometry = Constructors.geomFromText(geomString, fileDataSplitter);
        if(geometry.getGeometryType().contains("LineString")) {
            return geometry;
        } else {
            return null;
        }
    }

    public static Geometry polygonFromEnvelope(double minX, double minY, double maxX, double maxY) {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX, maxY);
        coordinates[2] = new Coordinate(maxX, maxY);
        coordinates[3] = new Coordinate(maxX, minY);
        coordinates[4] = coordinates[0];
        return GEOMETRY_FACTORY.createPolygon(coordinates);
    }

    public static Geometry geomFromGeoHash(String geoHash, Integer precision) {
        System.out.println(geoHash);
        System.out.println(precision);
        try {
            return GeoHashDecoder.decode(geoHash, precision);
        } catch (GeoHashDecoder.InvalidGeoHashException e) {
            return null;
        }
    }

    public static Geometry geomFromGML(String gml) throws IOException, ParserConfigurationException, SAXException {
        return new GMLReader().read(gml, GEOMETRY_FACTORY);
    }

    public static Geometry geomFromKML(String kml) throws ParseException {
        return new KMLReader().read(kml);
    }


}
