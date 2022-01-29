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
package org.apache.sedona.core.joinJudgement;

import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.*;

public class JudgementHelper {
    public static boolean match(Geometry left, Geometry right, HalfOpenRectangle extent, boolean considerBoundaryIntersection)
    {
        if (extent != null) {
            // Handle easy case: points. Since each point is assigned to exactly one partition,
            // different partitions cannot emit duplicate results.
            if (left instanceof Point || right instanceof Point) {
                return geoMatch(left, right, considerBoundaryIntersection);
            }

            // Neither geometry is a point

            // Check if reference point of the intersection of the bounding boxes lies within
            // the extent of this partition. If not, don't run any checks. Let the partition
            // that contains the reference point do all the work.
            Envelope intersection =
                    left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
            if (!intersection.isNull()) {
                final Point referencePoint =
                        makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
                if (!extent.contains(referencePoint)) {
                    return false;
                }
            }
        }

        return geoMatch(left, right, considerBoundaryIntersection);
    }

    private static Point makePoint(double x, double y, GeometryFactory factory)
    {
        return factory.createPoint(new Coordinate(x, y));
    }

    private static boolean geoMatch(Geometry left, Geometry right, boolean considerBoundaryIntersection)
    {
        //log.warn("Check "+left.toText()+" with "+right.toText());
        return considerBoundaryIntersection ? left.intersects(right) : left.covers(right);
    }
}
