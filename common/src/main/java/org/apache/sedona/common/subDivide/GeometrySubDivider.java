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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.sedona.common.simplify.GeometrySimplifier;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeometrySubDivider {
    private static GeometryFactory geometryFactory = new GeometryFactory();

    private static final double FP_TOLERANCE = 1e-12;

    private static final int maxDepth = 50;

    public static final int minMaxVertices = 5;

    public static boolean equalValue(double value, double pivot) {
        return Math.abs(value - pivot) > FP_TOLERANCE;
    }

    public static Geometry[] subDivideRecursive(
            Geometry geom,
            int dimension,
            int maxVertices,
            int depth,
            Geometry[] geometries
    ) {
        if (geom == null) {
            return geometries;
        }
        int numberOfVertices = geom.getNumPoints();
        Envelope geometryBbox = geom.getEnvelope().getEnvelopeInternal();
        double width = geometryBbox.getWidth();
        double height = geometryBbox.getHeight();
        Envelope geometryBboxAligned = null;
        if (width == 0) {
            geometryBboxAligned = new Envelope(
                    geometryBbox.getMinX() - FP_TOLERANCE,
                    geometryBbox.getMaxX() + FP_TOLERANCE,
                    geometryBbox.getMinY(),
                    geometryBbox.getMaxY()
            );
        } else if (height == 0) {
            geometryBboxAligned = new Envelope(
                    geometryBbox.getMinX(),
                    geometryBbox.getMaxX(),
                    geometryBbox.getMinY() - FP_TOLERANCE,
                    geometryBbox.getMaxY() + FP_TOLERANCE
            );
        } else {
            geometryBboxAligned = geometryBbox;
        }
        if (geom.getDimension() < dimension || numberOfVertices == 0) {
            return geometries;
        } else if (width == 0.0 && height == 0.0) {
            if (geom instanceof Point) {
                if (dimension == 0) {
                    return ArrayUtils.addAll(
                            new Geometry[] {geom},
                            geometries
                    );
                } else {
                    return geometries;
                }
            } else {
                return geometries;
            }
        } else if (geom instanceof GeometryCollection && !geom.getGeometryType().contains("Point")) {
            GeometryCollection geometryCollection = (GeometryCollection) geom;
            List<Geometry> dividedGeoms = new ArrayList<>();
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                dividedGeoms.addAll(
                        Arrays.asList(
                                subDivideRecursive(
                                        geometryCollection.getGeometryN(i),
                                        dimension,
                                        maxVertices,
                                        depth,
                                        geometries
                                )
                        )
                );
            }
            return dividedGeoms.toArray(new Geometry[0]);
        } else {
            if (numberOfVertices <= maxVertices || depth > maxDepth) {
                return ArrayUtils.addAll(
                        new Geometry[] {geom},
                        geometries
                );
            } else {
                boolean splitOrdinate = (width <= height);
                double center = splitOrdinate ?
                        (geometryBboxAligned.getMinY() + geometryBboxAligned.getMaxY()) / 2 :
                        (geometryBboxAligned.getMinX() + geometryBboxAligned.getMaxX()) / 2;
                double pivot = PivotFinder.findPivot(geom, splitOrdinate, center, numberOfVertices);
                SubBoxes subBoxes = getSubBoxes(splitOrdinate, new SubDivideExtent(geometryBboxAligned), pivot, center);
                Geometry intersectedSimplified = getIntersectionGeometries(subBoxes.getSubBox(), geom);
                Geometry intersectedIter2Simplified = getIntersectionGeometries(subBoxes.getSubBox2(), geom);

                if (intersectedSimplified != null && !intersectedSimplified.isEmpty() && intersectedIter2Simplified != null && !intersectedIter2Simplified.isEmpty()) {
                    return ArrayUtils.addAll(
                            subDivideRecursive(intersectedSimplified, dimension, maxVertices, depth + 1, geometries),
                            subDivideRecursive(intersectedIter2Simplified, dimension, maxVertices, depth + 1, geometries)
                    );
                } else if (intersectedSimplified != null && !intersectedSimplified.isEmpty()) {
                    return subDivideRecursive(intersectedSimplified, dimension, maxVertices, depth + 1, geometries);
                } else if (intersectedIter2Simplified != null && !intersectedIter2Simplified.isEmpty()) {
                    return subDivideRecursive(intersectedIter2Simplified, dimension, maxVertices, depth + 1, geometries);
                } else {
                    return geometries;
                }
            }
        }
    }
    private static SubBoxes getSubBoxes(boolean splitOrdinate, SubDivideExtent subbox, double pivot, double center) {
        if (splitOrdinate) {
            if (equalValue(subbox.getyMax(), pivot) && equalValue(subbox.getyMin(), pivot))
                return new SubBoxes(
                        subbox.copy().setyMax(pivot),
                        subbox.copy().setyMin(pivot)
                );
            else
                return new SubBoxes(subbox.copy().setyMax(center), subbox.copy().setyMin(center));
        } else {
            if (equalValue(subbox.getxMax(), pivot) && equalValue(subbox.getxMin(), pivot))
                return new SubBoxes(
                        subbox.copy().setxMax(pivot),
                        subbox.copy().setxMin(pivot)
                );
            else
                return new SubBoxes(subbox.copy().setxMax(center), subbox.copy().setxMin(center));
        }
    }

    private static Geometry getIntersectionGeometries(SubDivideExtent extent, Geometry geom) {
        Envelope subBox = new Envelope(
                extent.getxMin(),
                extent.getxMax(),
                extent.getyMin(),
                extent.getyMax()
        );
        Geometry intersected = geom.intersection(JTS.toGeometry(subBox));
        Geometry res = GeometrySimplifier.simplify(intersected, true, 0.0);
        return res;
    }

    public static Geometry[] subDivide(Geometry geom, int maxVertices) {
        if (geom == null || maxVertices < minMaxVertices) {
            return new Geometry[0];
        }
        return subDivideRecursive(geom, geom.getDimension(), maxVertices, 0, new Geometry[0]);
    }

}
