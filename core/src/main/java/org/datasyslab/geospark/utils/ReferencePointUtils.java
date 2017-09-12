package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import java.util.Objects;

public class ReferencePointUtils {

    /**
     * Calculates reference point for a geometry. Supports points, multi-points,
     * linestrings, multi-linestring, polygons, multi-polygons and collections of such.
     *
     * Throws an exception if input type is not one of the above.
     *
     * @param geometry input geometry; must not be null or empty
     * @return a point on the border of the specified geometry; guarantees to
     * return the same point for equal geometries.
     */
    public static Point getReferencePoint(Geometry geometry) {
        Objects.requireNonNull(geometry, "geometry can't be null");

        if (geometry.isEmpty()) {
            throw new IllegalArgumentException("Can't calculate reference point for an empty geometry");
        }

        if (geometry instanceof GeometryCollection) {
            final GeometryCollection collection = (GeometryCollection) geometry;
            Point reference = getReferencePointInt(collection.getGeometryN(0));
            for (int i=0; i<collection.getNumGeometries(); i++) {
                final Point candidate = getReferencePointInt(collection.getGeometryN(i));
                reference = min(candidate, reference);
            }
            return reference;
        }

        return getReferencePointInt(geometry);
    }

    private static Point getReferencePointInt(Geometry geometry) {
        if (geometry instanceof Point) {
            return (Point) geometry;
        }

        if (geometry instanceof LineString) {
            return getReferenceForLineString((LineString) geometry);
        }

        if (geometry instanceof Polygon) {
            final Polygon polygon = (Polygon) geometry;
            return getReferenceForPolygon(polygon);
        }

        throw new UnsupportedOperationException(
            "Can't calculate reference point for geometry type: " + geometry.getGeometryType());
    }

    private static Point getReferenceForPolygon(Polygon polygon) {
        return getReferenceForLineString(polygon.getExteriorRing());
    }

    private static Point getReferenceForLineString(LineString lineString) {
        Point reference = lineString.getStartPoint();
        for (int i=1; i<lineString.getNumPoints(); i++) {
            reference = min(lineString.getPointN(i), reference);
        }
        return reference;
    }

    private static Point min(Point a, Point b) {
        if (a.getX() < b.getX()) {
            return a;
        }

        if (a.getX() == b.getX() && a.getY() < b.getY()) {
            return a;
        }

        return b;
    }
}
