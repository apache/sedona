/*
 * FILE: JudgementBase
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;

public class JudgementBase<U extends Geometry>
        implements Serializable
{

    private static final Logger log = LogManager.getLogger(JudgementBase.class);
    private boolean considerBoundaryIntersection;
    U queryGeometry;
    protected boolean leftCoveredByRight = true;

    /**
     * Instantiates a new range filter using index.
     *
     * @param queryWindow the query window
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public JudgementBase(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.queryGeometry = queryWindow;
        this.leftCoveredByRight = leftCoveredByRight;
    }

    public boolean match(Geometry spatialObject, Geometry queryWindow)
    {
        if (considerBoundaryIntersection) {
            if (queryWindow.intersects(spatialObject)) { return true; }
        }
        else {
            if (queryWindow.covers(spatialObject)) { return true; }
        }
        return false;
    }
}
