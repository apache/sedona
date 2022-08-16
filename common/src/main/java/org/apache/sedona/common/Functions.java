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

import java.util.Optional;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.GeometryGeoHashEncoder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;



public class Functions {
    public static Geometry buffer(Geometry geometry, double radius) {
        return geometry.buffer(radius);
    }

    public static double distance(Geometry left, Geometry right) {
        return left.distance(right);
    }

    public static double xMin(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double min = Double.MAX_VALUE;
        for(int i=0; i < points.length; i++){
            min = Math.min(points[i].getX(), min);
        }
        return min;
    }
    
    public static double xMax(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double max = Double.MIN_VALUE;
        for (int i=0; i < points.length; i++) {
            max = Math.max(points[i].getX(), max);
        }
        return max;
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

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, false);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient)
        throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRScode = CRS.decode(sourceCRS);
        CoordinateReferenceSystem targetCRScode = CRS.decode(targetCRS);
        MathTransform transform = CRS.findMathTransform(sourceCRScode, targetCRScode, lenient);
        return JTS.transform(geometry, transform);
    }

    public static Geometry flipCoordinates(Geometry geometry) {
        GeomUtils.flipCoordinates(geometry);
        return geometry;
    }

    public static String geohash(Geometry geometry, int precision) {
        return GeometryGeoHashEncoder.calculate(geometry, precision);   
    }

    public static Geometry pointOnSurface(Geometry geometry) {
        return GeomUtils.getInteriorPoint(geometry);
    }

    public static Geometry reverse(Geometry geometry) {
        return geometry.reverse();
    }

    public static Geometry pointN(Geometry geometry, int n) {
        if(!(geometry instanceof LineString)) {
            return null;
        }
        return GeomUtils.getNthPoint((LineString)geometry, n);
    }

    public static Geometry exteriorRing(Geometry geometry) {
        return GeomUtils.getExteriorRing(geometry);
    }

    public static String asEWKT(Geometry geometry) {
        return GeomUtils.getEWKT(geometry);
    }

    public static Geometry force2D(Geometry geometry) {
        return GeomUtils.get2dGeom(geometry);
    }

    public static boolean isEmpty(Geometry geometry) {
        return geometry.isEmpty();
    }

    public static Geometry buildArea(Geometry geometry) {
        return GeomUtils.buildArea(geometry);
    }
}
