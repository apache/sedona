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

package org.apache.sedona.core.enums;

public enum SpatialJoinOptimizationMode {
    /**
     * Don't optimize spatial joins, just leave them as they are (cartesian join or broadcast nested loop join).
     */
    NONE,

    /**
     * Optimize all spatial joins, even though the join is an equi-join. For example, for a range join like this:
     * <p>{@code SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)}
     * <p>The join will still be optimized to a spatial range join.
     */
    ALL,

    /**
     * Optimize spatial joins that are not equi-join, this is the default mode.
     * <p>For example, for a range join like this:
         * <p>{@code SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)}
     * <p>It won't be optimized as a spatial join, since it is an equi-join (with equi-condition: {@code A.x = B.x}), and
     * could be executed by a sort-merge join or hash join.
     */
    NONEQUI;

    public static SpatialJoinOptimizationMode getSpatialJoinOptimizationMode(String str) {
        for (SpatialJoinOptimizationMode me : SpatialJoinOptimizationMode.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
