/**
 * FILE: PrimitiveShape.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.io.BytesWritable;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.*;

import java.io.IOException;
import java.io.Serializable;

public class PrimitiveShape implements Serializable{

    /** primitive bytes of one record copied from .shp file */
    private byte[] primitiveRecord = null;

    /** attributes of record extracted from .dbf file */
    private String attributes = null;

    /** shape type */
    public ShapeType shapeType = ShapeType.NULL;

    public byte[] getPrimitiveRecord() {
        return primitiveRecord;
    }

    public void setPrimitiveRecord(ShpRecord shpRecord) {
        this.primitiveRecord = shpRecord.getBytes().getBytes();
        shapeType = ShapeType.getType(shpRecord.getTypeID());
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public Geometry getShape(GeometryFactory geometryFactory) throws IOException, TypeUnknownException {
        ShapeParser parser = null;
        parser = shapeType.getParser(geometryFactory);
        if(parser == null) throw new TypeUnknownException(shapeType.getId());
        ShapeReader reader = new ByteBufferReader(primitiveRecord, false);
        Geometry shape = parser.parserShape(reader);
        if(attributes != null){
            shape.setUserData(attributes);
        }
        return shape;
    }

}
