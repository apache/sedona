package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.io.BytesWritable;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeFileConst;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShapeWritable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by zongsizhang on 6/19/17.
 */
public abstract class ShapeParser implements Serializable, ShapeFileConst{

    protected GeometryFactory geometryFactory = null;

    public ShapeParser(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * parse the shape to a geometry
     * @param reader
     */
    public abstract Geometry parserShape(ShapeReader reader) throws IOException;

    /**
     * read and parse the file header of .shp file
     * @param reader
     */
    public static void parseFileHeader(ShapeReader reader) throws IOException {
        int fileCode = reader.readInt();
        reader.skip(HEAD_EMPTY_NUM * INT_LENGTH);
        int fileLength = 16 * reader.readInt() - HEAD_FILE_LENGTH_16BIT * 16;
        int remainLength = fileLength;
        int fileVersion = EndianUtils.swapInteger(reader.readInt());
        int currentTokenType = EndianUtils.swapInteger(reader.readInt());
        reader.skip(HEAD_BOX_NUM * DOUBLE_LENGTH);
    }

    /**
     * read the primitive Content and put in byte array
     * @param reader
     * @return
     * @throws IOException
     */
    public static PrimitiveShapeWritable parseRecordPrimitiveContent(ShapeReader reader) throws IOException{
        PrimitiveShapeWritable primitiveShape = new PrimitiveShapeWritable();
        int contentLength = reader.readInt();
        ShapeType shapeType = ShapeType.getType(EndianUtils.swapInteger(reader.readInt()));
        long recordLength = 16 * (contentLength + 4);
        //remainLength -= recordLength;
        byte[] contentArray = new byte[contentLength * 2];
        reader.read(contentArray,0,contentArray.length);
        primitiveShape.setPrimitiveRecord(new BytesWritable(contentArray));
        primitiveShape.setShapeType(shapeType);
        return primitiveShape;
    }




}
