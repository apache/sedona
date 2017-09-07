/**
 * FILE: PolygonParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.PolygonParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonParser.
 */
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
    public Geometry parserShape(ShapeReader reader) throws IOException {
        reader.skip(4 * DOUBLE_LENGTH);
        int numRings = reader.readInt();
        int numPoints = reader.readInt();
        int[] ringsOffsets = new int[numRings+1];
        for(int i = 0;i < numRings; ++i){
            ringsOffsets[i] = reader.readInt();
        }
        ringsOffsets[numRings] = numPoints;

        //read all points out
        CoordinateSequence coordinateSequence = ShpParseUtil.readCoordinates(reader, numPoints, geometryFactory);

        List<LinearRing> holes = new ArrayList<LinearRing>();
        List<LinearRing> shells = new ArrayList<LinearRing>();

        // reading coordinates and assign to holes or shells
        for(int i = 0;i < numRings; ++i){
            int readScale = ringsOffsets[i+1] - ringsOffsets[i];
            CoordinateSequence csRing = geometryFactory.getCoordinateSequenceFactory().create(readScale,2);
            for(int j = 0;j < readScale; ++j){
                csRing.setOrdinate(j, 0, coordinateSequence.getOrdinate(ringsOffsets[i]+j, 0));
                csRing.setOrdinate(j, 1, coordinateSequence.getOrdinate(ringsOffsets[i]+j, 1));
            }
            if(csRing.size() < 3) continue; // if points less than 3, it's not a ring, we just abandon it
            LinearRing ring = geometryFactory.createLinearRing(csRing);
            if(CoordinateSequences.isCCW(csRing)){// is a hole
                holes.add(ring);
            }else{// current coordinate sequance is a
                shells.add(ring);
            }
        }
        // assign shells and holes to polygons
        Polygon[] polygons = null;
        //if only one shell, assign all holes to it directly. If there is no holes, we set each shell as polygon
        if(shells.size() == 1){
            polygons = new Polygon[]{
                    geometryFactory.createPolygon(shells.get(0), GeometryFactory.toLinearRingArray(holes))
            };
        }else if(shells.size() == 0 && holes.size() == 1){// if no shell, we set all holes as shell
            polygons = new Polygon[]{
                    geometryFactory.createPolygon(holes.get(0))
            };
        } else{// there are multiple shells and one or more holes, find which shell a hole is within
            List<ArrayList<LinearRing>> holesWithinShells = new ArrayList<ArrayList<LinearRing>>();
            for(int i = 0;i < shells.size(); ++i){
                holesWithinShells.add(new ArrayList<LinearRing>());
            }
            // for every hole, find home
            for(LinearRing hole : holes){
                //prepare test objects
                LinearRing testRing = hole;
                LinearRing minShell = null;
                Envelope minEnv = null;
                Envelope testEnv = testRing.getEnvelopeInternal();
                Coordinate testPt = testRing.getCoordinateN(0);
                LinearRing tryRing;

                for(LinearRing shell : shells){
                    tryRing = shell;
                    Envelope tryEnv = tryRing.getEnvelopeInternal();
                    if (minShell != null) {
                        minEnv = minShell.getEnvelopeInternal();
                    }
                    boolean isContained = false;
                    Coordinate[] coordList = tryRing.getCoordinates();
                    if (tryEnv.contains(testEnv)
                            && (CGAlgorithms.isPointInRing(testPt, coordList) || (ShpParseUtil.pointInList(
                            testPt, coordList)))) {
                        isContained = true;
                    }
                    if (isContained) {
                        if ((minShell == null) || minEnv.contains(tryEnv)) {
                            minShell = tryRing;
                        }
                    }
                }
                if(minShell == null){// no shell contains this hole, set it to shell
                    shells.add(hole);
                    holesWithinShells.add(new ArrayList<LinearRing>());
                }else{// contained by a shell, assign hole to it
                    holesWithinShells.get(shells.indexOf(minShell)).add(hole);
                }
            }
            // create multi polygon
            polygons = new Polygon[shells.size()];
            for(int i = 0;i < shells.size(); ++i){
                polygons[i] = geometryFactory.createPolygon(shells.get(i),
                        GeometryFactory.toLinearRingArray(holesWithinShells.get(i)));
            }
        }
        return geometryFactory.createMultiPolygon(polygons);
    }
}
