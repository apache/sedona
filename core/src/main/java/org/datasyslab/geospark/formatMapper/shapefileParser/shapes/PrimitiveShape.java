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

/**
 * Created by zongsizhang on 6/1/17.
 */
public class PrimitiveShape implements Serializable{

    /** primitive bytes of one record */
    private BytesWritable primitiveRecord = null;

    /** primitive bytes from one record */
    private BytesWritable primitiveAttribute = null;

    /** shape type */
    ShapeType shapeType = ShapeType.NULL;

    /** dbf field descriptor */
    List<FieldDescriptor> fieldDescriptorList = null;

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

    public void setPrimitiveAttribute(BytesWritable primitiveAttribute, List<FieldDescriptor> fieldDescriptors) {
        this.primitiveAttribute = primitiveAttribute;
        fieldDescriptorList = new ArrayList<>(fieldDescriptors);
    }

    public String generateAttributes(){
        String attrStr = "";
        if(primitiveAttribute != null){
            try{
                DataInputStream dbfInputStream = new DataInputStream(
                        new ByteArrayInputStream(primitiveAttribute.getBytes()));
                attrStr = DbfParseUtil.primitiveToAttributes(dbfInputStream, fieldDescriptorList);
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
        return parser.parserShape(reader);
    }

}
