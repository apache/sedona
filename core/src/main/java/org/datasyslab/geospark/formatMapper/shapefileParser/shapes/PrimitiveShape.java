/**
 * FILE: PrimitiveShape.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeParser;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReaderFactory;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class PrimitiveShape
        implements Serializable
{

    /**
     * primitive bytes of one record copied from .shp file
     */
    private final byte[] primitiveRecord;

    /**
     * shape type
     */
    private final ShapeType shapeType;

    /**
     * attributes of record extracted from .dbf file
     */
    private String attributes = null;

    public PrimitiveShape(ShpRecord record)
    {
        this.primitiveRecord = record.getBytes().getBytes();
        this.shapeType = ShapeType.getType(record.getTypeID());
    }

    public byte[] getPrimitiveRecord()
    {
        return primitiveRecord;
    }

    public String getAttributes()
    {
        return attributes;
    }

    public void setAttributes(String attributes)
    {
        this.attributes = attributes;
    }

    public Geometry getShape(GeometryFactory geometryFactory)
            throws IOException, TypeUnknownException
    {
        ShapeParser parser = shapeType.getParser(geometryFactory);
        ByteBuffer shapeBuffer = ByteBuffer.wrap(primitiveRecord);
        Geometry shape = parser.parseShape(ShapeReaderFactory.fromByteBuffer(shapeBuffer));
        if (attributes != null) {
            shape.setUserData(attributes);
        }
        return shape;
    }
}
