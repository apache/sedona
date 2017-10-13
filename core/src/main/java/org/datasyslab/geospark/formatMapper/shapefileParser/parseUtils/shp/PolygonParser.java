/**
 * FILE: PolygonParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.PolygonParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst.DOUBLE_LENGTH;

public class PolygonParser extends ShapeParser{

    /**
     * create a parser that can abstract a Polygon from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public PolygonParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract abstract a Polygon shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader) {
        reader.skip(4 * DOUBLE_LENGTH);

        int numRings = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numRings, numPoints);

        boolean shellsCCW = false;

        LinearRing shell = null;
        List<LinearRing> holes = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();

        for(int i = 0; i < numRings; ++i){
            int readScale = offsets[i+1] - offsets[i];
            CoordinateSequence csRing = readCoordinates(reader, readScale);

            if(csRing.size() < 3) {
                continue; // if points less than 3, it's not a ring, we just abandon it
            }

            LinearRing ring = geometryFactory.createLinearRing(csRing);
            if (shell == null) {
                shell = ring;
                shellsCCW = CoordinateSequences.isCCW(csRing);
            } else if (CoordinateSequences.isCCW(csRing) != shellsCCW) {
                holes.add(ring);
            } else {
                if (shell != null) {
                    Polygon polygon = geometryFactory.createPolygon(shell, GeometryFactory.toLinearRingArray(holes));
                    polygons.add(polygon);
                }

                shell = ring;
                holes.clear();
            }
        }

        if (shell != null) {
            Polygon polygon = geometryFactory.createPolygon(shell, GeometryFactory.toLinearRingArray(holes));
            polygons.add(polygon);
        }

        if (polygons.size() == 1) {
            return polygons.get(0);
        }

        return geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }
}
