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

package org.apache.sedona.common.enums;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum GeometryType.
 */
public enum GeometryType
        implements Serializable
{

    POINT,
    POLYGON,
    LINESTRING,
    MULTIPOINT,
    MULTIPOLYGON,
    MULTILINESTRING,
    GEOMETRYCOLLECTION,
    CIRCLE,
    RECTANGLE;

    /**
     * Gets the GeometryType.
     *
     * @param str the str
     * @return the GeometryType
     */
    public static GeometryType getGeometryType(String str)
    {
        for (GeometryType me : GeometryType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        throw new IllegalArgumentException("[" + GeometryType.class + "] Unsupported geometry type:" + str);
    }
}
