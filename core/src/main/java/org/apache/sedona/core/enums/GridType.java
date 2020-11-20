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

import org.apache.log4j.Logger;
import org.apache.sedona.core.formatMapper.FormatMapper;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum GridType.
 */
public enum GridType
        implements Serializable
{
    /**
     * The Quad-Tree partitioning.
     */
    QUADTREE,

    /**
     * K-D-B-tree partitioning (k-dimensional B-tree)
     */
    KDBTREE;

    /**
     * Gets the grid type.
     *
     * @param str the str
     * @return the grid type
     */
    public static GridType getGridType(String str)
    {
        final Logger logger = Logger.getLogger(GridType.class);
        for (GridType me : GridType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        logger.error("[Sedona] Choose quadtree or kdbtree instead. This grid type is not supported: " + str);
        return null;
    }
}
