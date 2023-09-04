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

package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeParser;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeReaderFactory;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeType;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

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
