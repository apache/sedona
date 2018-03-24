/**
 * FILE: GeometryType.java
 * PATH: org.datasyslab.geospark.enums.GeometryType.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.enums;

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
    CIRCLE;

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
