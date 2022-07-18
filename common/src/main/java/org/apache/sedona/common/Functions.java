package org.apache.sedona.common;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class Functions {
    public static double distance(Geometry left, Geometry right) {
        return left.distance(right);
    }

    public static double yMin(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double min = Double.MAX_VALUE;
        for(int i=0; i < points.length; i++){
            min = Math.min(points[i].getY(), min);
        }
        return min;
    }
    
    public static double yMax(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double max = Double.MIN_VALUE;
        for (int i=0; i < points.length; i++) {
            max = Math.max(points[i].getY(), max);
        }
        return max;
    }
}
