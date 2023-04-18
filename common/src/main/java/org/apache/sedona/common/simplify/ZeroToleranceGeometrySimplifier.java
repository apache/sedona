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

import java.util.ArrayList;
import java.util.List;

public class ZeroToleranceGeometrySimplifier {
    public static Coordinate[] simplifyInPlaceTolerance0(Coordinate[] geom) {
        List<Coordinate> resultArray = new ArrayList<>();
        int currentIndex = 1;
        int lastIndex = geom.length - 1;
        Coordinate keptPoint = geom[0];
        int keptIt = 0;
        while (currentIndex < lastIndex) {
            Coordinate currPt = geom[currentIndex];
            Coordinate nextPt = geom[currentIndex + 1];
            double ba_x = nextPt.x - keptPoint.x;
            double ba_y = nextPt.y - keptPoint.y;
            double ab_length_sqr = ba_x * ba_x + ba_y * ba_y;

            double ca_x = currPt.x - keptPoint.x;
            double ca_y = currPt.y - keptPoint.y;
            double dot_ac_ab = ca_x * ba_x + ca_y * ba_y;
            double s_numerator = ca_x * ba_y - ca_y * ba_x;

            boolean isEligible = dot_ac_ab < 0.0 || dot_ac_ab > ab_length_sqr || s_numerator != 0;

            if (keptIt != currentIndex && isEligible) resultArray.add(keptPoint);
            if (isEligible) keptPoint = currPt;
            if (isEligible) keptIt = keptIt + 1;
            currentIndex = currentIndex + 1;
        }
        if (keptIt != currentIndex) {
            resultArray.add(keptPoint);
            resultArray.add(geom[lastIndex]);
        } else {
            resultArray.add(geom[lastIndex]);
        }
        return resultArray.toArray(new Coordinate[0]);
    }
}
