package org.apache.sedona.common.simplify;

import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.utils.GeomUtils;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.Objects;

public class GeometryCollectionSimplifier extends BaseSimplifier{

    public static Geometry simplify(GeometryCollection geom, boolean preserveCollapsed, double epsilon) {

        Geometry[] simplifiedSubGeoms = Arrays.stream(GeomUtils.getSubGeometries(geom)).map(
                subGeom -> {
                    if (subGeom instanceof Circle) {
                        return CircleSimplifier.simplify( (Circle) subGeom, preserveCollapsed, epsilon);
                    }
                    else if (subGeom instanceof LineString) {
                        return LineStringSimplifier.simplify((LineString) subGeom, preserveCollapsed, epsilon);
                    }
                    else if (subGeom instanceof Point) {
                        return subGeom;
                    }
                    else if (subGeom instanceof Polygon) {
                        return PolygonSimplifier.simplify((Polygon) subGeom, preserveCollapsed, epsilon);
                    }
                    else if (subGeom instanceof GeometryCollection) {
                        return simplify((GeometryCollection) subGeom, preserveCollapsed, epsilon);
                    } else {
                        return null;
                    }
                }
        ).filter(Objects::nonNull).toArray(Geometry[]::new);

        String[] distinctGeometries = Arrays.stream(simplifiedSubGeoms).map(Geometry::getGeometryType).distinct().toArray(String[]::new);
        if (distinctGeometries.length == 1){
            switch (distinctGeometries[0]) {
                case Geometry.TYPENAME_LINESTRING:
                    return geometryFactory.createMultiLineString(Arrays.stream(simplifiedSubGeoms).map(x -> (LineString) x).toArray(LineString[]::new));
                case Geometry.TYPENAME_POLYGON:
                    return geometryFactory.createMultiPolygon(Arrays.stream(simplifiedSubGeoms).map(x -> (Polygon) x).toArray(Polygon[]::new));
                case Geometry.TYPENAME_POINT:
                    return geometryFactory.createMultiPoint(Arrays.stream(simplifiedSubGeoms).map(x -> (Point) x).toArray(Point[]::new));
            }
        }
        return geometryFactory.createGeometryCollection(simplifiedSubGeoms);
    }
}
