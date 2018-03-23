/*
 * FILE: PrimitiveShape
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
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
