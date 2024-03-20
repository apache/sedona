package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.geom.Point;
import java.util.List;
import java.util.Map;

public class RasterInterpolate {
    private RasterInterpolate() {}

    public static double interpolateIDW(int x, int y, Quadtree quadtree, Map<Point, Double> pointValueMap, double radius, double power) {
        List<?> queryResult = quadtree.query(new Envelope(x - radius, x + radius, y - radius, y + radius));
        GeometryFactory geometryFactory = new GeometryFactory();

        double numerator = 0.0;
        double denominator = 0.0;

        for (Object obj : queryResult) {
            Point point = (Point) obj;
            double distance = point.distance(geometryFactory.createPoint(new Coordinate(x, y)));

            if (distance <= radius && distance > 0) {
                double weight = 1.0 / Math.pow(distance, power);
                numerator += weight * pointValueMap.get(point);
                denominator += weight;
            }
        }
        double interpolatedValue = Math.round(denominator > 0 ? numerator / denominator : Double.NaN);
        return interpolatedValue;
    }


}
