/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.linearref.LinearGeometryBuilder;
import org.locationtech.jts.operation.polygonize.Polygonizer;


/**
 * Class to split geometry by other geometry.
 */
public final class GeometrySplitter {
    final static Logger logger = LoggerFactory.getLogger(GeometrySplitter.class);
    private final GeometryFactory geometryFactory;

    public GeometrySplitter(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * Split input geometry by the blade geometry.
     * Input geometry can be lineal (LineString or MultiLineString)
     * or polygonal (Polygon or MultiPolygon). A GeometryCollection
     * can also be used as an input but it must be homogeneous.
     * For lineal geometry refer to the
     * {@link splitLines(Geometry, Geometry) splitLines} method for
     * restrictions on the blade. Refer to
     * {@link splitPolygons(Geometry, Geometry) splitPolygons} for
     * restrictions on the blade for polygonal input geometry.
     * <p>
     * The result will be null if the input geometry and blade are either
     * invalid in general or in relation to each other. Otherwise, the result
     * will always be a MultiLineString or MultiPolygon depending on the input,
     * and even if the result is a single geometry.
     *
     * @param  input input geometry
     * @param  blade geometry to use as a blade
     * @return       multi-geometry resulting from the split or null if invalid
     */
    public GeometryCollection split(Geometry input, Geometry blade) {
        GeometryCollection result = null;

        if (GeomUtils.geometryIsLineal(input)) {
            result = splitLines(input, blade);
        } else if (GeomUtils.geometryIsPolygonal(input)) {
            result = splitPolygons(input, blade);
        }

        return result;
    }

    /**
     * Split linear input geometry by the blade geometry.
     * Input geometry is assumed to be either a LineString,
     * MultiLineString, or a homogeneous collection of lines in a
     * GeometryCollection. The blade geometry can be any individual
     * puntal, lineal, or polygonal geometry or homogeneous collection
     * of those geometries. Blades that are polygonal will use their
     * boundary for the split. Will always return a MultiLineString.
     *
     * @param  input input geometry to be split that must be lineal
     * @param  blade blade geometry to use for split
     *
     * @return       input geometry split by blade
     */
    public MultiLineString splitLines(Geometry input, Geometry blade) {
        MultiLineString result = null;

        if (GeomUtils.geometryIsPolygonal(blade)) {
            blade = blade.getBoundary();
        }

        if (GeomUtils.geometryIsPuntal(blade)) {
            result = splitLinesByPoints(input, blade);
        } else if (GeomUtils.geometryIsLineal(blade)) {
            result = splitLinesByLines(input, blade);
        }

        return result;
    }

    /**
     * Split polygonal input geometry by the blade geometry.
     * Input geometry is assumed to be either a Polygon, MultiPolygon,
     * or a GeometryCollection of only polygons. The blade geometry
     * can be any individual lineal or polygonal geometry or homogeneous
     * collection of those geometries. Blades that are polygonal
     * will use their boundary for the split. Will always return a
     * MultiPolygon.
     *
     * @param  input input polygonal geometry to split
     * @param  blade geometry to split the input by
     * @return       input geometry split by the blade
     */
    public MultiPolygon splitPolygons(Geometry input, Geometry blade) {
        MultiPolygon result = null;

        if (GeomUtils.geometryIsPolygonal(blade)) {
            blade = blade.getBoundary();
        }

        if (GeomUtils.geometryIsLineal(blade)) {
            result = splitPolygonsByLines(input, blade);
        }

        return result;
    }

    private MultiLineString splitLinesByPoints(Geometry lines, Geometry points) {
        // coords must be ordered for the algorithm in splitLineStringAtCoordinates
        // do it here so the sorting is only done once per split operation
        // use a deque for easy forward and reverse iteration
        Deque<Coordinate> pointCoords = getOrderedCoords(points);

        LinearGeometryBuilder lineBuilder = new LinearGeometryBuilder(geometryFactory);
        lineBuilder.setIgnoreInvalidLines(true);

        for (int lineIndex = 0; lineIndex < lines.getNumGeometries(); lineIndex++) {
            splitLineStringAtCoordinates((LineString)lines.getGeometryN(lineIndex), pointCoords, lineBuilder);
        }

        MultiLineString result = (MultiLineString)ensureMultiGeometryOfDimensionN(lineBuilder.getGeometry(), 1);

        return result;
    }

    private MultiLineString splitLinesByLines(Geometry inputLines, Geometry blade) {
        // compute the intersection of inputLines and blade
        // and pass back to splitLines to handle as points
        Geometry intersectionWithBlade = inputLines.intersection(blade);

        if (intersectionWithBlade.isEmpty()) {
            // blade and inputLines are disjoint so just return the input as a multilinestring
            return (MultiLineString)ensureMultiGeometryOfDimensionN(inputLines, 1);
        } else if (intersectionWithBlade.getDimension() != 0) {
            logger.warn("Colinear sections detected between source and blade geometry. Returned null.");
            return null;
        }

        return splitLines(inputLines, intersectionWithBlade);
    }

    private MultiPolygon splitPolygonsByLines(Geometry polygons, Geometry blade) {
        ArrayList<Polygon> validResults = new ArrayList<>();

        for (int polygonIndex = 0; polygonIndex < polygons.getNumGeometries(); polygonIndex++) {
            Geometry originalPolygon = polygons.getGeometryN(polygonIndex);
            Geometry candidatePolygons = generateCandidatePolygons(originalPolygon, blade);
            addValidPolygonsToList(candidatePolygons, originalPolygon, validResults);
        }

        return geometryFactory.createMultiPolygon(validResults.toArray(new Polygon[0]));
    }

    private Deque<Coordinate> getOrderedCoords(Geometry geometry) {
        Coordinate[] pointCoords = geometry.getCoordinates();
        ArrayDeque<Coordinate> coordsDeque = new ArrayDeque<>(pointCoords.length);

        // coords are ordered from left to right, bottom to top
        Arrays.sort(pointCoords);
        coordsDeque.addAll(Arrays.asList(pointCoords));

        return coordsDeque;
    }

    private void splitLineStringAtCoordinates(LineString line, Deque<Coordinate> pointCoords, LinearGeometryBuilder lineBuilder) {
        Coordinate[] lineCoords = line.getCoordinates();
        if (lineCoords.length > 1) {
            lineBuilder.add(lineCoords[0]);

            for (int endCoordIndex = 1; endCoordIndex < lineCoords.length; endCoordIndex++) {
                Coordinate endCoord = lineCoords[endCoordIndex];
                Iterator<Coordinate> coordIterator = getIteratorForSegmentDirection(pointCoords, lineBuilder.getLastCoordinate(), endCoord);

                applyCoordsToLineSegment(lineBuilder, coordIterator, endCoord);
            }

            lineBuilder.endLine();
        }
    }

    private Iterator<Coordinate> getIteratorForSegmentDirection(Deque<Coordinate> coords, Coordinate startCoord, Coordinate endCoord) {
        // line segments are assumed to be left to right, bottom to top
        // and coords is also ordered that way
        // however, if the line segment is backwards then coords will
        // need to be iterated in reverse
        Iterator<Coordinate> coordIterator;

        if (endCoord.compareTo(startCoord) == -1) {
            coordIterator = coords.descendingIterator();
        } else {
            coordIterator = coords.iterator();
        }

        return coordIterator;
    }

    private void applyCoordsToLineSegment(LinearGeometryBuilder lineBuilder, Iterator<Coordinate> coordIterator, Coordinate endCoord) {

        while (coordIterator.hasNext()) {
            Coordinate pointCoord = coordIterator.next();
            Coordinate lastCoord = lineBuilder.getLastCoordinate();

            if (coordIsOnLineSegment(pointCoord, lastCoord, endCoord)) {
                splitOnCoord(lineBuilder, pointCoord);
            }

        }

        lineBuilder.add(endCoord, false);
    }

    private boolean coordIsOnLineSegment(Coordinate coord, Coordinate startCoord, Coordinate endCoord) {
        boolean result = false;
        boolean segmentIsVertical = startCoord.x == endCoord.x;
        boolean coordIsWithinVerticalRange = Math.min(startCoord.y, endCoord.y) <= coord.y && coord.y <= Math.max(startCoord.y, endCoord.y);

        if (coordIsWithinVerticalRange) {
            if (segmentIsVertical) {
                if (coord.x == startCoord.x) {
                    result = true;
                }
            } else {
                double m = (startCoord.y - endCoord.y) / (startCoord.x - endCoord.x);
                double yInt = startCoord.y - startCoord.x * m;
                if (coord.y == coord.x * m + yInt) {
                    result = true;
                }
            }
        }

        return result;
    }

    private void splitOnCoord(LinearGeometryBuilder lineBuilder, Coordinate coord) {
        lineBuilder.add(coord, false);
        lineBuilder.endLine();
        lineBuilder.add(coord, true);
    }

    private Geometry generateCandidatePolygons(Geometry polygons, Geometry blade) {
        // restrict the blade to only be within the original polygon to
        // avoid candidate polygons that are impossible
        Geometry bladeWithinPolygons = blade.intersection(polygons);

        // a union will node all of the lines at intersections
        // these nodes are required for Polygonizer to work correctly
        Geometry totalLineWork = polygons.getBoundary().union(bladeWithinPolygons);

        Polygonizer polygonizer = new Polygonizer();
        polygonizer.add(totalLineWork);

        return polygonizer.getGeometry();
    }

    private void addValidPolygonsToList(Geometry polygons, Geometry original, List<Polygon> list) {
        // polygons must be checked to make sure they are contained within the
        // original geometry to ensure holes in the original geometry are excluded
        for (int i = 0; i < polygons.getNumGeometries(); i++) {
            Geometry candidateResult = polygons.getGeometryN(i);
            if (candidateResult instanceof Polygon && original.contains(candidateResult)) {
                list.add((Polygon)candidateResult);
            }
        }
    }

    private GeometryCollection ensureMultiGeometryOfDimensionN(Geometry geometry, int dimension) {
        GeometryCollection result = null;

        if (geometry != null) {
            if (dimension == 0 && geometry instanceof MultiPoint) {
                result = (MultiPoint) geometry;
            } else if (dimension == 1 && geometry instanceof MultiLineString) {
                result = (MultiLineString) geometry;
            } else if (dimension == 2 && geometry instanceof MultiPolygon) {
                result = (MultiPolygon) geometry;
            } else {
                ArrayList<Geometry> validGeometries = new ArrayList<>();

                for (int n = 0; n < geometry.getNumGeometries(); n++) {
                    Geometry candidateGeometry = geometry.getGeometryN(n);
                    if (candidateGeometry.getDimension() == dimension) {
                        validGeometries.add(candidateGeometry);
                    }
                }

                switch (dimension) {
                    case 0:
                        result = (GeometryCollection)geometryFactory.createMultiPoint(validGeometries.toArray(new Point[0]));
                        break;
                    case 1:
                        result = (GeometryCollection)geometryFactory.createMultiLineString(validGeometries.toArray(new LineString[0]));
                        break;
                    case 2:
                        result = (GeometryCollection)geometryFactory.createMultiPolygon(validGeometries.toArray(new Polygon[0]));
                        break;
                }
            }
        }

        return result;
    }
}
