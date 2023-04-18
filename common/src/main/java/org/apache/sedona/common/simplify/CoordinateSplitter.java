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
package org.apache.sedona.common.simplify;

import org.locationtech.jts.geom.Coordinate;

public class CoordinateSplitter {

    private static final double DBL_EPSILON = 2.2204460492503131e-16;

    public static SplitInPlace splitInPlace(
            Coordinate[] geom,
            int itFirst,
            int itLast,
            double maxDistanceSquared
    ) {
        int split = itFirst;
        double maxDistance = maxDistanceSquared;
        if ((itFirst - itLast) < 2) new SplitInPlace(geom, itFirst);
        Coordinate pointA = geom[itFirst];
        Coordinate pointB = geom[itLast];
        if (calculateSquaredDistance(pointA, pointB) < DBL_EPSILON){
            for (int itk = itFirst + 1; itk < itLast ; itk++){
                Coordinate pk = geom[itk];
                double squaredDistance = calculateSquaredDistance(pk, pointA);
                if (squaredDistance > maxDistance){
                    split = itk;
                    maxDistance = squaredDistance;
                }
            }
        }

        double ba_x = pointB.x - pointA.x;
        double ba_y = pointB.y - pointA.y;
        double ab_length_sqr = (ba_x * ba_x + ba_y * ba_y);
        maxDistance *= ab_length_sqr;
        for (int itk = itFirst + 1; itk < itLast; itk++){
            Coordinate c = geom[itk];
            double distance_sqr = 0.0;
            double ca_x = c.x - pointA.x;
            double ca_y = c.y - pointA.y;
            double dot_ac_ab = (ca_x * ba_x + ca_y * ba_y);
            if (dot_ac_ab <= 0.0) {
                distance_sqr = calculateSquaredDistance(c, pointA) * ab_length_sqr;
            }
            else if (dot_ac_ab >= ab_length_sqr) {
                distance_sqr = calculateSquaredDistance(c, pointB) * ab_length_sqr;
            }
            else {
                double s_numerator = ca_x * ba_y - ca_y * ba_x;
                distance_sqr = s_numerator * s_numerator;
            }
            if (distance_sqr > maxDistance)
            {
                split = itk;
                maxDistance = distance_sqr;
            }
        }
        return new SplitInPlace(geom, split);
    }

    private static double calculateSquaredDistance(Coordinate geomA, Coordinate geomB) {
        double distance = geomA.distance(geomB);
        return distance * distance;
    }

    public static class SplitInPlace {
        private Coordinate[] geom;

        private int split;

        public SplitInPlace(Coordinate[] geom, int split) {
            this.geom = geom;
            this.split = split;
        }

        public Coordinate[] getGeom() {
            return geom;
        }

        public int getSplit() {
            return split;
        }
    }
}
