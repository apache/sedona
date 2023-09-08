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
package org.apache.sedona.viz.utils;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Enum ColorizeOption.
 */
public enum ColorizeOption
        implements Serializable
{

    /**
     * The earthobservation.
     */
    EARTHOBSERVATION("EARTHOBSERVATION"),

    /**
     * The spatialaggregation.
     */
    SPATIALAGGREGATION("spatialaggregation"),

    /**
     * The normal.
     */
    NORMAL("normal");

    /**
     * The type name.
     */
    private String typeName = "normal";

    /**
     * Instantiates a new colorize option.
     *
     * @param typeName the type name
     */
    ColorizeOption(String typeName)
    {
        this.setTypeName(typeName);
    }

    /**
     * Gets the colorize option.
     *
     * @param str the str
     * @return the colorize option
     */
    public static ColorizeOption getColorizeOption(String str)
    {
        for (ColorizeOption me : ColorizeOption.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }

    /**
     * Gets the type name.
     *
     * @return the type name
     */
    public String getTypeName()
    {
        return typeName;
    }

    /**
     * Sets the type name.
     *
     * @param typeName the new type name
     */
    public void setTypeName(String typeName)
    {
        this.typeName = typeName;
    }
}
