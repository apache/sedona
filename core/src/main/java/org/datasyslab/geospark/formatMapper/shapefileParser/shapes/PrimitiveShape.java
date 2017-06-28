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
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PrimitiveShape implements Serializable{

    /** primitive bytes of one record */
    private BytesWritable primitiveRecord = null;

    /** primitive bytes from one record */
    private BytesWritable primitiveAttribute = null;

    /** shape type */
    ShapeType shapeType = ShapeType.NULL;

    public BytesWritable getPrimitiveRecord() {
        return primitiveRecord;
    }

    public void setPrimitiveRecord(ShpRecord shpRecord) {
        this.primitiveRecord = shpRecord.getBytes();
        shapeType = ShapeType.getType(shpRecord.getTypeID());
    }

    public BytesWritable getPrimitiveAttribute() {
        return primitiveAttribute;
    }

    public void setPrimitiveAttribute(BytesWritable primitiveAttribute) {
        this.primitiveAttribute = primitiveAttribute;
    }

    public String generateAttributes(){
        String attrStr = "";
        if(primitiveAttribute != null){
            try{
                DataInputStream dbfInputStream = new DataInputStream(
                        new ByteArrayInputStream(primitiveAttribute.getBytes()));
                attrStr = DbfParseUtil.primitiveToAttributes(dbfInputStream);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        return attrStr;
    }

    public Geometry getShape(GeometryFactory geometryFactory) throws IOException, TypeUnknownException {
        ShapeParser parser = null;
        parser = shapeType.getParser(geometryFactory);
        if(parser == null) throw new TypeUnknownException(shapeType.getId());
        ShapeReader reader = new ByteBufferReader(primitiveRecord.getBytes(), false);
        Geometry shape = parser.parserShape(reader);
        if(primitiveAttribute != null){
            String attributes = generateAttributes();
            shape.setUserData(attributes);
        }
        return shape;
    }

}
