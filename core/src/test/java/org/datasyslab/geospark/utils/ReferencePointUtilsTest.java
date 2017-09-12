package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReferencePointUtilsTest {

    private final GeometryFactory factory = new GeometryFactory();

    @Test
    public void testPoint() throws ParseException {
        final Point point = makePoint(1.2, 30.4);
        assertEquals(point, getReferencePoint(point));
    }

    @Test
    public void testMultiPoint() throws ParseException {
        {
            final MultiPoint multiPoint = (MultiPoint) parseWKT("MULTIPOINT (0.1 0.3, 1 1)");
            assertEquals(makePoint(0.1, 0.3), getReferencePoint(multiPoint));
        }

        {
            final MultiPoint multiPoint = (MultiPoint) parseWKT("MULTIPOINT (0 0, 1 1, -1 2)");
            assertEquals(makePoint(-1, 2), getReferencePoint(multiPoint));
        }

        {
            final MultiPoint multiPoint = (MultiPoint) parseWKT("MULTIPOINT (0 0, 1 1, -1 2, -0.5 -0.4)");
            assertEquals(makePoint(-1, 2), getReferencePoint(multiPoint));
        }
    }

    @Test
    public void testLineString() throws ParseException {
        {
            final LineString lineString = parseLineString("LINESTRING (0.1 0.3, 1 1)");
            assertEquals(makePoint(0.1, 0.3), getReferencePoint(lineString));
        }

        {
            final LineString lineString = parseLineString("LINESTRING (0 0, 1 1, -1 2)");
            assertEquals(makePoint(-1, 2), getReferencePoint(lineString));
        }

        {
            final LineString lineString = parseLineString("LINESTRING (0 0, 1 1, -1 2, -0.5 -0.4)");
            assertEquals(makePoint(-1, 2), getReferencePoint(lineString));
        }
    }

    @Test
    public void testMultiLineString() throws ParseException {
        final MultiLineString mLineString = (MultiLineString) parseWKT(
            "MULTILINESTRING ((0.1 0.2, 1 1), (0 1, 2 5), (-1 0.5, -1 -1))");
        assertEquals(makePoint(-1, -1), getReferencePoint(mLineString));
    }

    @Test
    public void testPolygon() throws ParseException {
        {
            final Polygon polygon = (Polygon) parseWKT("POLYGON ((0.1 0.2, 1 1, 1 0, 0.1 0.2))");
            assertEquals(makePoint(0.1, 0.2), getReferencePoint(polygon));
        }

        // Same polygon as above, but defined using a different starting point
        {
            final Polygon polygon = (Polygon) parseWKT("POLYGON ((1 1, 1 0, 0.1 0.2, 1 1))");
            assertEquals(makePoint(0.1, 0.2), getReferencePoint(polygon));
        }

        {
            final Polygon polygon = (Polygon) parseWKT("POLYGON ((0.1 0.2, 0 1, -1 0.5, 0.1 0.2))");
            assertEquals(makePoint(-1, 0.5), getReferencePoint(polygon));
        }
    }

    @Test
    public void testMultiPolygon() throws ParseException {
        {
            final MultiPolygon polygon =
                    (MultiPolygon) parseWKT("MULTIPOLYGON (((0.1 0.2, 1 1, 1 0, 0.1 0.2)), " +
                            "((1 1, 1 2, 2 2, 2 1, 1 1)))");
            assertEquals(makePoint(0.1, 0.2), getReferencePoint(polygon));
        }

        {
            final MultiPolygon polygon =
                    (MultiPolygon) parseWKT("MULTIPOLYGON (((1 1, 1 2, 2 2, 2 1, 1 1)), " +
                            "((0.1 0.2, 1 1, 1 0, 0.1 0.2)))");
            assertEquals(makePoint(0.1, 0.2), getReferencePoint(polygon));
        }
    }

    @Test
    public void testGeometryCollection() throws ParseException {
        final GeometryCollection collection = (GeometryCollection) parseWKT(
            "GEOMETRYCOLLECTION (" +
                "POINT (0.1 0.2), " +
                "LINESTRING (0 0, 1 1, -1 2, -0.5 -0.4), " +
                "POLYGON ((0.1 0.2, 0 1, -1 0.5, 0.1 0.2))" +
            ")");
        assertEquals(makePoint(-1, 0.5), getReferencePoint(collection));
    }

    private Geometry parseWKT(String wkt) throws ParseException {
        return new WKTReader().read(wkt);
    }

    private LineString parseLineString(String wkt) throws ParseException {
        return (LineString) parseWKT(wkt);
    }

    private Point getReferencePoint(Geometry geometry) {
        return ReferencePointUtils.getReferencePoint(geometry);
    }

    private Point makePoint(double x, double y) {
        return factory.createPoint(new Coordinate(x, y));
    }
}
