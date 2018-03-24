/**
 * FILE: ShapeKey.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class ShapeKey
        implements Serializable
{

    /**
     * record id
     */
    Long index = 0l;

    public void write(DataOutput dataOutput)
            throws IOException
    {
        dataOutput.writeLong(index);
    }

    public long getIndex()
    {
        return index;
    }

    public void setIndex(long _index)
    {
        index = _index;
    }
}
