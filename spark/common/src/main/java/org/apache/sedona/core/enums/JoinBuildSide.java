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

/**
 * The enumerator type of JoinQuery JoinBuildSide. Each join query has two sides, left shape and right shape.
 * The join side decides which side the spatial index is built on. The other side will be streamed out.
 */
public enum JoinBuildSide
        implements Serializable
{
    LEFT,
    RIGHT;

    public static JoinBuildSide getBuildSide(String str)
    {
        for (JoinBuildSide me : JoinBuildSide.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
