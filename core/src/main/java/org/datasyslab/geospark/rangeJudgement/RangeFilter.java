/*
 * FILE: RangeFilter
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.rangeJudgement;

import org.locationtech.jts.geom.Geometry;
import org.apache.spark.api.java.function.Function;

// TODO: Auto-generated Javadoc

public class RangeFilter<U extends Geometry, T extends Geometry>
        extends JudgementBase
        implements Function<T, Boolean>
{

    public RangeFilter(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        super(queryWindow, considerBoundaryIntersection, leftCoveredByRight);
    }

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Boolean call(T geometry)
            throws Exception
    {
        if (leftCoveredByRight) {
            return match(geometry, queryGeometry);
        }
        else {
            return match(queryGeometry, queryGeometry);
        }
    }
}
