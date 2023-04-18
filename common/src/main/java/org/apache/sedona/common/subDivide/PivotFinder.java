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
package org.apache.sedona.common.subDivide;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

public class PivotFinder {

    private static final double DBL_MAX = Double.MAX_VALUE;

    private static GeometryFactory geometryFactory = new GeometryFactory();

    public static double findPivot(
            Geometry geom,
            boolean splitOrdinate,
            double center,
            int numberOfVertices
    ) {
        double pivot = DBL_MAX;
        if (geom instanceof Polygon) {
            double pivotEps = DBL_MAX;
            double ptEps;
            Polygon lwPoly = (Polygon) geom.copy();
            // by default use the shell
            LinearRing ringToTrim = lwPoly.getExteriorRing();
            // if the shell is too small, use the largest hole
            if (numberOfVertices >= 2 * lwPoly.getExteriorRing().getNumPoints()) {
                // find the hole with largest area and assign to ringtotrim
                double maxArea = geometryFactory.createPolygon(lwPoly.getExteriorRing()).getArea();
                for (int i = 0; i < lwPoly.getNumInteriorRing(); i++) {
                    LinearRing curHole = lwPoly.getInteriorRingN(i);
                    double holeArea = geometryFactory.createPolygon(curHole).getArea();
                    if (holeArea > maxArea) {
                        maxArea = holeArea;
                        ringToTrim = curHole;
                    }
                }
            }
            for (int i = 0;i < ringToTrim.getNumPoints(); i++) {
                double pt = splitOrdinate ? ringToTrim.getPointN(i).getY() : ringToTrim.getPointN(i).getX();
                ptEps = Math.abs(pt - center);
                if (pivotEps > ptEps) {
                    pivot = pt;
                    pivotEps = ptEps;
                }
            }
        }
        if (pivot == DBL_MAX) return center;
        else return pivot;
    }
}
