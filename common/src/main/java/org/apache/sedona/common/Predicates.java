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

import org.locationtech.jts.geom.*;
import org.apache.sedona.common.sphere.Spheroid;

import java.util.concurrent.atomic.AtomicBoolean;

public class Predicates {
    public static boolean contains(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.contains(rightGeometry);
    }
    public static boolean intersects(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.intersects(rightGeometry);
    }
    public static boolean within(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.within(rightGeometry);
    }
    public static boolean covers(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.covers(rightGeometry);
    }
    public static boolean coveredBy(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.coveredBy(rightGeometry);
    }
    public static boolean crosses(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.crosses(rightGeometry);
    }
    public static boolean overlaps(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.overlaps(rightGeometry);
    }
    public static boolean touches(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.touches(rightGeometry);
    }
    public static boolean equals(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.symDifference(rightGeometry).isEmpty();
    }
    public static boolean disjoint(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.disjoint(rightGeometry);
    }
    public static boolean orderingEquals(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.equalsExact(rightGeometry);
    }
    public static boolean dWithin(Geometry leftGeometry, Geometry rightGeometry, double distance) {
       return dWithin(leftGeometry, rightGeometry, distance, false);
    }

    public static boolean dWithin(Geometry leftGeometry, Geometry rightGeometry, double distance, boolean useSpheroid) {
        if (useSpheroid) {
            double distanceSpheroid = Spheroid.distance(leftGeometry, rightGeometry);
            return distanceSpheroid <= distance;
        }else {
            return leftGeometry.isWithinDistance(rightGeometry, distance);
        }
    }

    /**
     * Checks if a geometry crosses the International Date Line.
     *
     * @param geometry The geometry to check.
     * @return True if the geometry crosses the Date Line, false otherwise.
     */
    public static boolean crossesDateLine(Geometry geometry) {
        if (geometry == null || geometry.isEmpty()) {
            return false;
        }

        AtomicBoolean crossesDateLine = new AtomicBoolean(false);

        CoordinateSequenceFilter filter = new CoordinateSequenceFilter() {
            private Coordinate previous = null;

            @Override
            public void filter(CoordinateSequence seq, int i) {
                if (i == 0) {
                    previous = seq.getCoordinateCopy(i);
                    return;
                }

                Coordinate current = seq.getCoordinateCopy(i);
                if (Math.abs(current.x - previous.x) > 180) {
                    crossesDateLine.set(true);
                }

                previous = current;
            }

            @Override
            public boolean isDone() {
                return crossesDateLine.get();
            }

            @Override
            public boolean isGeometryChanged() {
                return false;
            }
        };

        if (geometry instanceof GeometryCollection) {
            GeometryCollection collection = (GeometryCollection) geometry;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                Geometry part = collection.getGeometryN(i);
                part.apply(filter);
                if (crossesDateLine.get()) {
                    return true;
                }
            }
        } else {
            geometry.apply(filter);
        }

        return crossesDateLine.get();
    }


//    public static boolean crossesDateLine(Geometry geometry) {
//        if (geometry == null || geometry.isEmpty()) {
//            return false;
//        }
//
//        boolean crossesDateLine = false;
//        Coordinate previous = null;
//
//        for (Coordinate coord : geometry.getCoordinates()) {
//            if (previous != null && Math.abs(coord.x - previous.x) > 180) {
//                crossesDateLine = true;
//                break;
//            }
//            previous = coord;
//        }
//
//        return crossesDateLine;
//    }
}
