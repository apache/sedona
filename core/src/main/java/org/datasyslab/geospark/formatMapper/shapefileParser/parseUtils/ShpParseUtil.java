package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;
import io.netty.buffer.ByteBuf;
import org.apache.commons.io.EndianUtils;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeParser;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zongsizhang on 5/3/17.
 */
public class ShpParseUtil implements ShapeFileConst{

    public static CoordinateSequence readCoordinates(ShapeReader reader, int numPoints, GeometryFactory geometryFactory) throws IOException {
        CoordinateSequence coordinateSequence = geometryFactory.getCoordinateSequenceFactory().create(numPoints,2);
        double[] ordinates = new double[numPoints*2];
        reader.read(ordinates);
        for(int i = 0;i < numPoints; ++i){
            coordinateSequence.setOrdinate(i, 0, ordinates[i*2]);
            coordinateSequence.setOrdinate(i, 1, ordinates[i*2 + 1]);
        }
        return coordinateSequence;
    }

    /**
     * returns true if testPoint is a point in the pointList list.
     * @param testPoint
     * @param pointList
     * @return
     */
    public static boolean pointInList(Coordinate testPoint, Coordinate[] pointList) {
        Coordinate p;

        for (int t = pointList.length - 1; t >= 0; t--) {
            p = pointList[t];

            // nan test; x!=x iff x is nan
            if ((testPoint.x == p.x)
                    && (testPoint.y == p.y)
                    && ((testPoint.z == p.z) || (!(testPoint.z == testPoint.z)))
                    ) {
                return true;
            }
        }

        return false;
    }

    public static Geometry primitiveToShape(byte[] sourceBytes, GeometryFactory geometryFactory) throws IOException {
        ShapeReader reader = new ByteBufferReader(sourceBytes, false);
        ShapeType type = ShapeType.getType(reader.readInt());
        ShapeParser parser = type.getParser(geometryFactory);
        return parser.parserShape(reader);
    }


}
