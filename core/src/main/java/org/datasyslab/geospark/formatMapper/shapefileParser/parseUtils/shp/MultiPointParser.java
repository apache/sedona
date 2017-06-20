package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShpParseUtil;

import java.io.IOException;

/**
 * Created by zongsizhang on 6/19/17.
 */
public class MultiPointParser extends ShapeParser {

    public MultiPointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    @Override
    public Geometry parserShape(ShapeReader reader) throws IOException {
        reader.skip(4 * DOUBLE_LENGTH);
        int numPoints = reader.readInt();
        CoordinateSequence coordinateSequence = ShpParseUtil.readCoordinates(reader, numPoints, geometryFactory);
        MultiPoint multiPoint = geometryFactory.createMultiPoint(coordinateSequence);
        return multiPoint;
    }
}
