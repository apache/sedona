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

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum IndexType.
 */
public enum IndexType
        implements Serializable
{

    /**
     * The quadtree.
     */
    QUADTREE,

    /**
     * The rtree.
     */
    RTREE;

    /**
     * Gets the index type.
     *
     * @param str the str
     * @return the index type
     */
    public static IndexType getIndexType(String str)
    {
        for (IndexType me : IndexType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
