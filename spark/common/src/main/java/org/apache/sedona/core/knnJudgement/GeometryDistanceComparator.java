/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.knnJudgement;

import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Comparator;

// TODO: Auto-generated Javadoc

/**
 * The Class GeometryDistanceComparator.
 */
public class GeometryDistanceComparator<T extends Geometry>
        implements Comparator<T>, Serializable
{

    /**
     * The query center.
     */
    T queryCenter;

    /**
     * The normal order.
     */
    boolean normalOrder;

    /**
     * Instantiates a new geometry distance comparator.
     *
     * @param queryCenter the query center
     * @param normalOrder the normal order
     */
    public GeometryDistanceComparator(T queryCenter, boolean normalOrder)
    {
        this.queryCenter = queryCenter;
        this.normalOrder = normalOrder;
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(T p1, T p2)
    {
        double distance1 = (p1).distance(queryCenter);
        double distance2 = (p2).distance(queryCenter);
        if (this.normalOrder) {
            if (distance1 > distance2) {
                return 1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return -1;
        }
        else {
            if (distance1 > distance2) {
                return -1;
            }
            else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
}
