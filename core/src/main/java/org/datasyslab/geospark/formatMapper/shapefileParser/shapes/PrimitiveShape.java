package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.io.BytesWritable;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ByteBufferReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeParser;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;

import java.io.IOException;
import java.io.Serializable;

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

    public ShapeType getShapeType() {
        return shapeType;
    }

    public Geometry getShape(GeometryFactory geometryFactory) throws IOException {
        ShapeParser parser = shapeType.getParser(geometryFactory);
        ShapeReader reader = new ByteBufferReader(primitiveRecord.getBytes(), false);
        return parser.parserShape(reader);
    }

}
