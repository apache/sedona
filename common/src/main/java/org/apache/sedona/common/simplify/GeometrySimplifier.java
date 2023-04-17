package org.apache.sedona.common.simplify;

import org.apache.sedona.common.geometryObjects.Circle;
import org.locationtech.jts.geom.*;

public class GeometrySimplifier {
    public static Geometry simplify(Geometry geom, boolean preserveCollapsed, double epsilon) {
        if (geom instanceof Circle) {
            return CircleSimplifier.simplify((Circle) geom, preserveCollapsed, epsilon);
        } else if (geom instanceof GeometryCollection) {
            return GeometryCollectionSimplifier.simplify((GeometryCollection) geom, preserveCollapsed, epsilon);
        } else if (geom instanceof LineString) {
            return LineStringSimplifier.simplify(geom, preserveCollapsed, epsilon);
        } else if (geom instanceof Polygon) {
            return PolygonSimplifier.simplify((Polygon) geom, preserveCollapsed, epsilon);
        }else {
            return geom;
        }
    }
}
