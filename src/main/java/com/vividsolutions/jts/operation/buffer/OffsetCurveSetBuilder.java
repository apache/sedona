


/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.operation.buffer;

/**
 * @version 1.7
 */
import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geomgraph.*;
import com.vividsolutions.jts.noding.*;

/**
 * Creates all the raw offset curves for a buffer of a {@link Geometry}.
 * Raw curves need to be noded together and polygonized to form the final buffer area.
 *
 * @version 1.7
 */
public class OffsetCurveSetBuilder {


  private Geometry inputGeom;
  private double distance;
  private OffsetCurveBuilder curveBuilder;

  private List curveList = new ArrayList();

  public OffsetCurveSetBuilder(
      Geometry inputGeom,
          double distance,
          OffsetCurveBuilder curveBuilder)
  {
    this.inputGeom = inputGeom;
    this.distance = distance;
    this.curveBuilder = curveBuilder;
  }

  /**
   * Computes the set of raw offset curves for the buffer.
   * Each offset curve has an attached {@link Label} indicating
   * its left and right location.
   *
   * @return a Collection of SegmentStrings representing the raw buffer curves
   */
  public List getCurves()
  {
    add(inputGeom);
    return curveList;
  }

  /**
   * Creates a {@link SegmentString} for a coordinate list which is a raw offset curve,
   * and adds it to the list of buffer curves.
   * The SegmentString is tagged with a Label giving the topology of the curve.
   * The curve may be oriented in either direction.
   * If the curve is oriented CW, the locations will be:
   * <br>Left: Location.EXTERIOR
   * <br>Right: Location.INTERIOR
   */
  private void addCurve(Coordinate[] coord, int leftLoc, int rightLoc)
  {
    // don't add null or trivial curves
    if (coord == null || coord.length < 2) return;
    // add the edge for a coordinate list which is a raw offset curve
    SegmentString e = new NodedSegmentString(coord,
                        new Label(0, Location.BOUNDARY, leftLoc, rightLoc));
    curveList.add(e);
  }


  private void add(Geometry g)
  {
    if (g.isEmpty()) return;

    if (g instanceof Polygon)                 addPolygon((Polygon) g);
                        // LineString also handles LinearRings
    else if (g instanceof LineString)         addLineString((LineString) g);
    else if (g instanceof Point)              addPoint((Point) g);
    else if (g instanceof MultiPoint)         addCollection((MultiPoint) g);
    else if (g instanceof MultiLineString)    addCollection((MultiLineString) g);
    else if (g instanceof MultiPolygon)       addCollection((MultiPolygon) g);
    else if (g instanceof GeometryCollection) addCollection((GeometryCollection) g);
    else  throw new UnsupportedOperationException(g.getClass().getName());
  }
  private void addCollection(GeometryCollection gc)
  {
    for (int i = 0; i < gc.getNumGeometries(); i++) {
      Geometry g = gc.getGeometryN(i);
      add(g);
    }
  }
  /**
   * Add a Point to the graph.
   */
  private void addPoint(Point p)
  {
    // a zero or negative width buffer of a line/point is empty
    if (distance <= 0.0) 
      return;
    Coordinate[] coord = p.getCoordinates();
    Coordinate[] curve = curveBuilder.getLineCurve(coord, distance);
    addCurve(curve, Location.EXTERIOR, Location.INTERIOR);
  }
  
  private void addLineString(LineString line)
  {
    // a zero or negative width buffer of a line/point is empty
    if (distance <= 0.0 && ! curveBuilder.getBufferParameters().isSingleSided()) 
      return;
    Coordinate[] coord = CoordinateArrays.removeRepeatedPoints(line.getCoordinates());
    Coordinate[] curve = curveBuilder.getLineCurve(coord, distance);
    addCurve(curve, Location.EXTERIOR, Location.INTERIOR);

    // TESTING
    //Coordinate[] curveTrim = BufferCurveLoopPruner.prune(curve); 
    //addCurve(curveTrim, Location.EXTERIOR, Location.INTERIOR);
  }

  private void addPolygon(Polygon p)
  {
    double offsetDistance = distance;
    int offsetSide = Position.LEFT;
    if (distance < 0.0) {
      offsetDistance = -distance;
      offsetSide = Position.RIGHT;
    }

    LinearRing shell = (LinearRing) p.getExteriorRing();
    Coordinate[] shellCoord = CoordinateArrays.removeRepeatedPoints(shell.getCoordinates());
    // optimization - don't bother computing buffer
    // if the polygon would be completely eroded
    if (distance < 0.0 && isErodedCompletely(shell, distance))
        return;
    // don't attemtp to buffer a polygon with too few distinct vertices
    if (distance <= 0.0 && shellCoord.length < 3)
    	return;

    addPolygonRing(
            shellCoord,
            offsetDistance,
            offsetSide,
            Location.EXTERIOR,
            Location.INTERIOR);

    for (int i = 0; i < p.getNumInteriorRing(); i++) {

      LinearRing hole = (LinearRing) p.getInteriorRingN(i);
      Coordinate[] holeCoord = CoordinateArrays.removeRepeatedPoints(hole.getCoordinates());

      // optimization - don't bother computing buffer for this hole
      // if the hole would be completely covered
      if (distance > 0.0 && isErodedCompletely(hole, -distance))
          continue;

      // Holes are topologically labelled opposite to the shell, since
      // the interior of the polygon lies on their opposite side
      // (on the left, if the hole is oriented CCW)
      addPolygonRing(
            holeCoord,
            offsetDistance,
            Position.opposite(offsetSide),
            Location.INTERIOR,
            Location.EXTERIOR);
    }
  }
  
  /**
   * Adds an offset curve for a polygon ring.
   * The side and left and right topological location arguments
   * assume that the ring is oriented CW.
   * If the ring is in the opposite orientation,
   * the left and right locations must be interchanged and the side flipped.
   *
   * @param coord the coordinates of the ring (must not contain repeated points)
   * @param offsetDistance the distance at which to create the buffer
   * @param side the side of the ring on which to construct the buffer line
   * @param cwLeftLoc the location on the L side of the ring (if it is CW)
   * @param cwRightLoc the location on the R side of the ring (if it is CW)
   */
  private void addPolygonRing(Coordinate[] coord, double offsetDistance, int side, int cwLeftLoc, int cwRightLoc)
  {
    // don't bother adding ring if it is "flat" and will disappear in the output
    if (offsetDistance == 0.0 && coord.length < LinearRing.MINIMUM_VALID_SIZE)
      return;
    
    int leftLoc  = cwLeftLoc;
    int rightLoc = cwRightLoc;
    if (coord.length >= LinearRing.MINIMUM_VALID_SIZE 
        && CGAlgorithms.isCCW(coord)) {
      leftLoc = cwRightLoc;
      rightLoc = cwLeftLoc;
      side = Position.opposite(side);
    }
    Coordinate[] curve = curveBuilder.getRingCurve(coord, side, offsetDistance);
    addCurve(curve, leftLoc, rightLoc);
  }

  /**
   * The ringCoord is assumed to contain no repeated points.
   * It may be degenerate (i.e. contain only 1, 2, or 3 points).
   * In this case it has no area, and hence has a minimum diameter of 0.
   *
   * @param ringCoord
   * @param offsetDistance
   * @return
   */
  private boolean isErodedCompletely(LinearRing ring, double bufferDistance)
  {
    Coordinate[] ringCoord = ring.getCoordinates();
    double minDiam = 0.0;
    // degenerate ring has no area
    if (ringCoord.length < 4)
      return bufferDistance < 0;

    // important test to eliminate inverted triangle bug
    // also optimizes erosion test for triangles
    if (ringCoord.length == 4)
      return isTriangleErodedCompletely(ringCoord, bufferDistance);

    // if envelope is narrower than twice the buffer distance, ring is eroded
    Envelope env = ring.getEnvelopeInternal();
    double envMinDimension = Math.min(env.getHeight(), env.getWidth());
    if (bufferDistance < 0.0
        && 2 * Math.abs(bufferDistance) > envMinDimension)
      return true;

    return false;
    /**
     * The following is a heuristic test to determine whether an
     * inside buffer will be eroded completely.
     * It is based on the fact that the minimum diameter of the ring pointset
     * provides an upper bound on the buffer distance which would erode the
     * ring.
     * If the buffer distance is less than the minimum diameter, the ring
     * may still be eroded, but this will be determined by
     * a full topological computation.
     *
     */
//System.out.println(ring);
/* MD  7 Feb 2005 - there's an unknown bug in the MD code, so disable this for now
    MinimumDiameter md = new MinimumDiameter(ring);
    minDiam = md.getLength();
    //System.out.println(md.getDiameter());
    return minDiam < 2 * Math.abs(bufferDistance);
    */
  }

  /**
   * Tests whether a triangular ring would be eroded completely by the given
   * buffer distance.
   * This is a precise test.  It uses the fact that the inner buffer of a
   * triangle converges on the inCentre of the triangle (the point
   * equidistant from all sides).  If the buffer distance is greater than the
   * distance of the inCentre from a side, the triangle will be eroded completely.
   *
   * This test is important, since it removes a problematic case where
   * the buffer distance is slightly larger than the inCentre distance.
   * In this case the triangle buffer curve "inverts" with incorrect topology,
   * producing an incorrect hole in the buffer.
   *
   * @param triangleCoord
   * @param bufferDistance
   * @return
   */
  private boolean isTriangleErodedCompletely(
      Coordinate[] triangleCoord,
      double bufferDistance)
  {
    Triangle tri = new Triangle(triangleCoord[0], triangleCoord[1], triangleCoord[2]);
    Coordinate inCentre = tri.inCentre();
    double distToCentre = CGAlgorithms.distancePointLine(inCentre, tri.p0, tri.p1);
    return distToCentre < Math.abs(bufferDistance);
  }



}
