/**
 * FILE: ShpParseUtil.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpParseUtil.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.IOException;

public class ShpParseUtil implements ShapeFileConst {

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

}
